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
    // Cache the IP for each agent at the moment we last saw it on the
    // ring. When the agent transitions to status=draining, the watcher
    // removes it from RingState and ring.GetIp() returns null — but we
    // still need the IP to pull data FROM the leaving pod. The cache
    // gives us a few-second window where we can reach the still-alive
    // draining pod to stream its content into the survivors.
    private Dictionary<string, string> _lastAgentIps = new(StringComparer.Ordinal);
    private Task? _statusPollTask;

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
        _statusPollTask = Task.Run(() => PollAdoptionStatusAsync(_cts.Token));
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            _cts?.Cancel();
            var tasks = new List<Task>();
            if (_runner != null) tasks.Add(_runner);
            if (_statusPollTask != null) tasks.Add(_statusPollTask);
            if (tasks.Count > 0)
            {
                await Task.WhenAny(Task.WhenAll(tasks), Task.Delay(TimeSpan.FromSeconds(3), cancellationToken));
            }
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

                    // Snapshot the previous IP cache before mutating it so
                    // HandleTopologyChange can resolve "where did the
                    // leaving agent live?" — even if its etcd entry has
                    // been deleted by now, our cache still has the IP.
                    var prevIps = new Dictionary<string, string>(_lastAgentIps, StringComparer.Ordinal);

                    if (_lastAgents.Count > 0)
                    {
                        await HandleTopologyChange(_lastAgents, newAgents, ring, currentVersion, prevIps, token);
                    }
                    else
                    {
                        Console.WriteLine("[RebalanceCoordinator] first observation, no handoffs needed");
                    }

                    _lastTopologyVersion = currentVersion;
                    _lastAgents = newAgents.ToList();
                    _lastAgentIps = newAgents.ToDictionary(a => a, a => ring.GetIp(a) ?? "", StringComparer.Ordinal);
                }
                else
                {
                    // Even when the topology version hasn't moved, refresh
                    // the IP cache. A pod restart can swap an agent's IP
                    // without a membership delta (same pod name, same
                    // vnodes, just a new pod IP); we want the cache to
                    // track that so a future drain has accurate fall-back
                    // pull addresses.
                    var ring = RingState.Current;
                    if (ring.Agents.Count > 0)
                    {
                        _lastAgentIps = ring.Agents.ToDictionary(a => a, a => ring.GetIp(a) ?? "", StringComparer.Ordinal);
                    }
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
        Dictionary<string, string> previousIps,
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
            // We still continue so leftAgents handling can run with the
            // currentAgents fan-out (e.g. cluster reset where everyone
            // restarted with fresh storage and we need anti-entropy).
        }

        foreach (var newAgent in newAgents)
        {
            if (remainingPrev.Count == 0) break;
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
                await DispatchAdoption(dstIp, sourcePod, srcIp, 0, uint.MaxValue, (ulong)topologyVersion, leavingPod: null, dstName: newAgent, token);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[RebalanceCoordinator] dispatch {sourcePod} → {newAgent} failed: {ex.Message}");
            }
        }

        // ── Leaving-agent handoffs (drain protocol) ────────────────────
        // When an agent transitions to status=draining the watcher drops
        // it from the ring → it lands in `leftAgents` here. R=3 means
        // the leaving pod's data already has 2 surviving copies, but to
        // keep R intact we need at least one more agent to acquire its
        // content before the pod terminates.
        //
        // v1 strategy: dispatch a full-keyspace adoption on EVERY
        // remaining agent, pulling FROM the leaving pod (which is still
        // alive during drain — that's the whole point of the protocol).
        // This is intentionally over-replicated: it guarantees correctness
        // without per-vnode reasoning. RocksDB dedupes incoming chunks
        // the dst already had, so the only cost is bandwidth.
        //
        // The agent's /admin/retire endpoint blocks until
        // GetAdoptionStatus on every dispatched adoption reports
        // COMPLETED — at which point the pod is safe to terminate.
        if (leftAgents.Count > 0)
        {
            await HandleLeftAgents(leftAgents, currentAgents, ring, previousIps, topologyVersion, token);
        }
    }

    private static async Task HandleLeftAgents(
        IReadOnlyList<string> leftAgents,
        IReadOnlyList<string> currentAgents,
        ConsistentHashRing ring,
        Dictionary<string, string> previousIps,
        long topologyVersion,
        CancellationToken token)
    {
        if (currentAgents.Count == 0)
        {
            Console.WriteLine("[RebalanceCoordinator] HandleLeftAgents: no surviving agents — cannot dispatch handoffs (cluster has no targets)");
            return;
        }

        // Resolve the headless service name once. We use it to construct
        // per-pod stable DNS names for any leaving agent whose IP isn't
        // in our snapshot — typical for a pod that crashed before we
        // observed its 'draining' transition.
        var headless = Environment.GetEnvironmentVariable("AGENTS_LOADBALANCER");

        foreach (var leavingPod in leftAgents)
        {
            // Prefer the cached IP from the moment it was last on the
            // ring; fall back to constructing the StatefulSet pod DNS.
            string sourceIp = "";
            if (previousIps.TryGetValue(leavingPod, out var cachedIp) && !string.IsNullOrEmpty(cachedIp))
            {
                sourceIp = cachedIp;
            }
            else if (!string.IsNullOrWhiteSpace(headless))
            {
                // StatefulSet stable DNS: <pod>.<headless-svc-fqdn>
                sourceIp = $"{leavingPod}.{headless}";
                Console.WriteLine($"[RebalanceCoordinator] no cached IP for leaving {leavingPod} → falling back to DNS {sourceIp}");
            }
            else
            {
                Console.WriteLine($"[RebalanceCoordinator] HandleLeftAgents: leaving={leavingPod} has no cached IP and AGENTS_LOADBALANCER unset → cannot pull from it. Skipping handoff (anti-entropy will repair.)");
                continue;
            }

            int dispatched = 0, failed = 0;
            foreach (var dstName in currentAgents)
            {
                if (dstName == leavingPod) continue; // sanity

                var dstIp = ring.GetIp(dstName);
                if (string.IsNullOrEmpty(dstIp))
                {
                    Console.WriteLine($"[RebalanceCoordinator] HandleLeftAgents: skipping {leavingPod} → {dstName}: missing dst IP");
                    failed++;
                    continue;
                }

                try
                {
                    await DispatchAdoption(
                        dstIp: dstIp,
                        sourcePod: leavingPod,
                        sourceIp: sourceIp,
                        hashRangeLow: 0,
                        hashRangeHigh: uint.MaxValue,
                        topologyVersion: (ulong)topologyVersion,
                        leavingPod: leavingPod, // tracker key
                        dstName: dstName,
                        token: token);
                    dispatched++;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[RebalanceCoordinator] HandleLeftAgents dispatch {leavingPod} → {dstName} failed: {ex.GetType().Name}: {ex.Message}");
                    failed++;
                }
            }

            Console.WriteLine($"[RebalanceCoordinator] HandleLeftAgents: leaving={leavingPod} dispatched={dispatched} failed={failed} (sourceIp={sourceIp})");
        }
    }

    private static async Task DispatchAdoption(
        string dstIp,
        string sourcePod,
        string sourceIp,
        uint hashRangeLow,
        uint hashRangeHigh,
        ulong topologyVersion,
        string? leavingPod,
        string dstName,
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
        Console.WriteLine($"[RebalanceCoordinator] adoption {res.AdoptionId} accepted by dst {dstIp} (leavingPod={leavingPod ?? "n/a"})");

        // For drain handoffs, register the adoption with the
        // RetirementTracker so the agent's /admin/retire poll loop can
        // observe completion. For "join" handoffs (leavingPod==null) we
        // still don't track completion; that's an open todo carried
        // over from Phase 4.
        if (!string.IsNullOrEmpty(leavingPod))
        {
            RetirementTracker.RegisterAdoption(
                leavingPod: leavingPod,
                adoptionId: res.AdoptionId,
                dstName: dstName,
                dstIp: dstIp,
                sourceIp: sourceIp);
        }
    }

    /// <summary>
    /// Background loop that polls every dst agent for adoption
    /// completion. Updates <see cref="RetirementTracker"/> when an
    /// adoption transitions to COMPLETED or FAILED so the agent's
    /// /admin/retire endpoint can release the preStop hook.
    ///
    /// Cadence:
    ///   - 3s polls for any tracked adoption that's still RUNNING.
    ///   - Skips already-terminal adoptions to avoid network thrash.
    ///   - Best-effort: a missed poll just means the agent waits a bit
    ///     longer; it doesn't break correctness because the next tick
    ///     will catch up.
    ///
    /// Memory:
    ///   The tracker is also pruned here every 60s to drop entries that
    ///   haven't seen activity in 30 minutes — safety net for cases
    ///   where the agent vanished without us hearing about completion.
    /// </summary>
    private async Task PollAdoptionStatusAsync(CancellationToken token)
    {
        var pollInterval = TimeSpan.FromSeconds(3);
        DateTime lastPrune = DateTime.MinValue;

        while (!token.IsCancellationRequested)
        {
            try
            {
                var pods = RetirementTracker.ListTrackedPods();
                foreach (var pod in pods)
                {
                    foreach (var (adoptionId, dstIp, status) in RetirementTracker.ListAdoptions(pod))
                    {
                        if (status != "RUNNING") continue;

                        try
                        {
                            var address = dstIp.Contains("://") ? dstIp : $"http://{dstIp}:5000";
                            using var channel = GrpcChannel.ForAddress(address, new GrpcChannelOptions
                            {
                                Credentials = global::Grpc.Core.ChannelCredentials.Insecure
                            });
                            var client = new VnodeStreamingService.VnodeStreamingServiceClient(channel);
                            var statusRes = await client.GetAdoptionStatusAsync(
                                new GetAdoptionStatus_Req { AdoptionId = adoptionId },
                                deadline: DateTime.UtcNow.AddSeconds(5),
                                cancellationToken: token);

                            string mapped = statusRes.Status switch
                            {
                                GetAdoptionStatus_Res.Types.Status.Completed => "COMPLETED",
                                GetAdoptionStatus_Res.Types.Status.Failed => "FAILED",
                                _ => "RUNNING"
                            };
                            if (mapped == "RUNNING")
                            {
                                RetirementTracker.TouchAdoption(pod, adoptionId);
                            }
                            else
                            {
                                RetirementTracker.MarkAdoptionTerminal(pod, adoptionId, mapped, statusRes.ErrorMessage);
                                Console.WriteLine($"[RebalanceCoordinator] adoption {adoptionId} (pod={pod}, dst={dstIp}) → {mapped}{(string.IsNullOrEmpty(statusRes.ErrorMessage) ? "" : ": " + statusRes.ErrorMessage)}");
                            }
                        }
                        catch (OperationCanceledException) when (token.IsCancellationRequested) { return; }
                        catch (Exception ex)
                        {
                            // Don't mark terminal — transient errors
                            // (channel reset, dst restart) shouldn't be
                            // recorded as failures. Just touch and move on.
                            RetirementTracker.TouchAdoption(pod, adoptionId);
                            // Log occasionally to avoid spam.
                            if (DateTime.UtcNow.Second % 10 == 0)
                            {
                                Console.WriteLine($"[RebalanceCoordinator] poll {adoptionId} (pod={pod}, dst={dstIp}) error: {ex.GetType().Name}: {ex.Message}");
                            }
                        }
                    }
                }

                if (DateTime.UtcNow - lastPrune > TimeSpan.FromMinutes(1))
                {
                    var pruned = RetirementTracker.PruneOlderThan(TimeSpan.FromMinutes(30));
                    if (pruned > 0) Console.WriteLine($"[RebalanceCoordinator] pruned {pruned} stale retirement records");
                    lastPrune = DateTime.UtcNow;
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested) { return; }
            catch (Exception ex)
            {
                Console.WriteLine($"[RebalanceCoordinator] PollAdoptionStatusAsync iteration error: {ex.GetType().Name}: {ex.Message}");
            }

            try { await Task.Delay(pollInterval, token); }
            catch (OperationCanceledException) { return; }
        }
    }

    private static bool IsEnabled()
    {
        var raw = Environment.GetEnvironmentVariable("REBALANCE_COORDINATOR_ENABLED");
        return string.Equals(raw, "true", StringComparison.OrdinalIgnoreCase)
            || string.Equals(raw, "1", StringComparison.Ordinal);
    }

    public void Dispose() { _cts?.Dispose(); }
}
