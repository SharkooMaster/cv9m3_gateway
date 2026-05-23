using Gateway.Routing;
using Grpc.Net.Client;

namespace Gateway.Services;

/// <summary>
/// Watches <see cref="RingState.TopologyVersion"/> and orchestrates
/// vnode handoffs when the consistent-hash ring's membership changes.
///
/// Responsibilities:
///   - Polls RingState every few seconds for topology version bumps.
///   - On a bump, diffs the previous and current rings: which agents
///     are new, which agents are gone.
///   - For each newly-joined agent, dispatches a <c>BeginVnodeAdoption</c>
///     RPC asking the new agent to pull data from one of the existing
///     replicas. The dst agent does the actual streaming work; this
///     coordinator only kicks off and tracks adoptions.
///
/// Why a single coordinator (not per-agent self-coordination):
///   With self-coordination, every newly-joined agent races to pull
///   from the same set of source agents → thundering herd. A single
///   coordinator schedules adoptions sequentially (configurable),
///   keeping rebalance bandwidth bounded.
///
/// Why on the gateway and not on cross:
///   Gateway already maintains the etcd watcher and ring, and is the
///   write fan-out point. Cross runs as a stateless front-end and
///   gets recycled by the HPA — letting cross own a long-running
///   rebalance task would make scaling cross down racy.
///
/// v1 scope:
///   - Pull-based ("the new agent pulls from one peer").
///   - Whole-chunk-store stream (no hash-range filter on the source).
///   - Sequential adoptions (one at a time).
///   - Logs progress; doesn't yet bump etcd's <c>/topology/version</c>
///     after completion (Phase 5 anti-entropy will close the loop).
/// </summary>
public class RebalanceCoordinator : IHostedService, IDisposable
{
    private CancellationTokenSource? _cts;
    private Task? _runner;

    private long _lastTopologyVersion = 0;
    private IReadOnlyList<string> _lastAgents = Array.Empty<string>();

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (!IsEnabled())
        {
            Console.WriteLine("[RebalanceCoordinator] disabled (REBALANCE_COORDINATOR_ENABLED != true) — skipping");
            return Task.CompletedTask;
        }

        Console.WriteLine("[RebalanceCoordinator] starting");
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _runner = Task.Run(() => RunAsync(_cts.Token));
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            _cts?.Cancel();
            if (_runner != null)
                await Task.WhenAny(_runner, Task.Delay(TimeSpan.FromSeconds(3), cancellationToken));
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[RebalanceCoordinator] StopAsync error: {ex.Message}");
        }
    }

    private async Task RunAsync(CancellationToken token)
    {
        // Poll cadence: short enough to react quickly to topology
        // changes, long enough that a flapping agent doesn't trigger
        // thrashing rebalance dispatches.
        var pollInterval = TimeSpan.FromSeconds(5);

        while (!token.IsCancellationRequested)
        {
            try
            {
                long currentVersion = RingState.TopologyVersion;
                if (currentVersion > _lastTopologyVersion)
                {
                    var ring = RingState.Current;
                    var newAgents = ring.Agents;
                    Console.WriteLine(
                        $"[RebalanceCoordinator] topology change detected: v{_lastTopologyVersion} → v{currentVersion}, " +
                        $"agents={newAgents.Count}");

                    if (_lastAgents.Count > 0)
                    {
                        await HandleTopologyChange(_lastAgents, newAgents, ring, currentVersion, token);
                    }
                    else
                    {
                        Console.WriteLine("[RebalanceCoordinator] first observation, no handoffs needed");
                    }

                    _lastTopologyVersion = currentVersion;
                    _lastAgents = newAgents.ToList();
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[RebalanceCoordinator] error in poll loop: {ex.GetType().Name}: {ex.Message}");
            }

            try { await Task.Delay(pollInterval, token); }
            catch (OperationCanceledException) { return; }
        }
    }

    private static async Task HandleTopologyChange(
        IReadOnlyList<string> previousAgents,
        IReadOnlyList<string> currentAgents,
        ConsistentHashRing ring,
        long topologyVersion,
        CancellationToken token)
    {
        var prevSet = new HashSet<string>(previousAgents, StringComparer.Ordinal);
        var newAgents = currentAgents.Where(a => !prevSet.Contains(a)).ToList();
        var leftAgents = previousAgents.Where(a => !currentAgents.Contains(a, StringComparer.Ordinal)).ToList();

        Console.WriteLine(
            $"[RebalanceCoordinator] diff: +{newAgents.Count} (joined: {string.Join(",", newAgents)}), " +
            $"-{leftAgents.Count} (left: {string.Join(",", leftAgents)})");

        // For each newly-joined agent, kick off a pull from one existing
        // peer. We pick the peer deterministically (first remaining
        // previousAgent) so two coordinators wouldn't disagree if both
        // ran. In practice only one gateway runs the coordinator at a
        // time (configurable), but determinism keeps testing simple.
        var remainingPrev = previousAgents.Where(a => currentAgents.Contains(a, StringComparer.Ordinal)).ToList();
        if (remainingPrev.Count == 0)
        {
            Console.WriteLine("[RebalanceCoordinator] no previous agents remain — cluster is bootstrapping; nothing to copy");
            return;
        }

        foreach (var newAgent in newAgents)
        {
            var sourcePod = remainingPrev[0];
            var dstIp = ring.GetIp(newAgent);
            var srcIp = ring.GetIp(sourcePod);
            if (string.IsNullOrEmpty(dstIp) || string.IsNullOrEmpty(srcIp))
            {
                Console.WriteLine(
                    $"[RebalanceCoordinator] skipping handoff {sourcePod} → {newAgent}: " +
                    $"missing IP (src={srcIp ?? "null"}, dst={dstIp ?? "null"})");
                continue;
            }

            // v1: ask dst to pull the entire keyspace. dst will filter
            // on receive (Phase 4 v1.1 will tighten this to per-vnode
            // hash ranges so we don't ship chunks the dst doesn't own).
            try
            {
                Console.WriteLine($"[RebalanceCoordinator] dispatching {sourcePod} → {newAgent} (full-keyspace v1)");
                await DispatchAdoption(dstIp, sourcePod, srcIp, 0, uint.MaxValue, (ulong)topologyVersion, token);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[RebalanceCoordinator] dispatch {sourcePod} → {newAgent} failed: {ex.Message}");
            }
        }
    }

    private static async Task DispatchAdoption(
        string dstIp,
        string sourcePod,
        string sourceIp,
        uint hashRangeLow,
        uint hashRangeHigh,
        ulong topologyVersion,
        CancellationToken token)
    {
        var address = dstIp.Contains("://") ? dstIp : $"http://{dstIp}:5000";
        using var channel = GrpcChannel.ForAddress(address, new GrpcChannelOptions
        {
            Credentials = global::Grpc.Core.ChannelCredentials.Insecure,
            MaxReceiveMessageSize = 256 * 1024 * 1024,
            MaxSendMessageSize = 256 * 1024 * 1024
        });

        var client = new VnodeStreamingService.VnodeStreamingServiceClient(channel);

        var req = new BeginVnodeAdoption_Req
        {
            SourcePod = sourcePod,
            SourceIp = sourceIp,
            HashRangeLow = hashRangeLow,
            HashRangeHigh = hashRangeHigh,
            TopologyVersion = topologyVersion
        };

        var res = await client.BeginVnodeAdoptionAsync(
            req,
            deadline: DateTime.UtcNow.AddSeconds(30),
            cancellationToken: token);

        if (!res.Accepted)
        {
            Console.WriteLine($"[RebalanceCoordinator] dst rejected adoption: {res.RejectReason}");
            return;
        }
        Console.WriteLine($"[RebalanceCoordinator] adoption {res.AdoptionId} accepted by dst {dstIp}");

        // We don't block on completion here — adoptions can take minutes
        // for a large dataset and blocking the coordinator on each one
        // would serialise the cluster. A future improvement is to poll
        // GetAdoptionStatus and bump etcd's /topology/version once
        // every dst reports completion.
    }

    private static bool IsEnabled()
    {
        var raw = Environment.GetEnvironmentVariable("REBALANCE_COORDINATOR_ENABLED");
        return string.Equals(raw, "true", StringComparison.OrdinalIgnoreCase)
            || string.Equals(raw, "1", StringComparison.Ordinal);
    }

    public void Dispose() { _cts?.Dispose(); }
}
