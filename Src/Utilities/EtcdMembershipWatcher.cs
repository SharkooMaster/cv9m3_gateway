using System.Text.Json;
using dotnet_etcd;
using Etcdserverpb;
using Gateway.Routing;
using Google.Protobuf;
using Mvccpb;

namespace Gateway.Utilities;

/// <summary>
/// Gateway-side mirror of cross/src/Utilities/EtcdMembershipWatcher.cs.
///
/// Why we duplicate this on gateway:
///   Gateway used to derive agent membership purely from
///   `Dns.GetHostAddresses(AgentsLoadbalancer)` + GetNodeInfo polls every
///   15 s. That's the same drift bug cross had — two gateway pods could
///   observe slightly different agent sets and route the same write
///   batch to different replicas, breaking write-quorum guarantees once
///   we land replicated writes.
///
///   This watcher subscribes to the same `/agents/` prefix in etcd and
///   atomically swaps a fresh <see cref="ConsistentHashRing"/> into
///   <see cref="RingState"/> on every membership event. Now both cross
///   and gateway see the same linearised stream and produce identical
///   ring snapshots.
///
/// Failure modes (same as cross's):
///   - etcd unreachable at startup: keeps retrying with backoff. Gateway
///     starts up regardless; PickReplicas will return Empty.Agents until
///     the snapshot lands.
///   - Watch stream drops mid-flight: re-snapshots from a fresh range
///     read, then re-attaches the watch from the new revision.
///   - Malformed key/value: skipped with a log line, doesn't kill the
///     stream.
/// </summary>
public class EtcdMembershipWatcher : IHostedService
{
    private readonly string? _endpoint;
    private CancellationTokenSource? _cts;
    private Task? _runner;
    private EtcdClient? _etcd;

    private readonly Dictionary<string, string> _members = new();
    private readonly Dictionary<string, int> _vnodesPerMember = new();

    public EtcdMembershipWatcher()
    {
        _endpoint = Environment.GetEnvironmentVariable("ETCD_ENDPOINT");
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(_endpoint))
        {
            Console.WriteLine(
                "[Gateway/EtcdMembershipWatcher] ETCD_ENDPOINT unset — etcd-driven membership disabled. " +
                "Gateway will fall back to legacy DNS+GetNodeInfo routing.");
            return;
        }

        Console.WriteLine($"[Gateway/EtcdMembershipWatcher] starting; etcd endpoint = {_endpoint}");
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        // ── Bounded wait for the first snapshot ──
        // If the watch runs purely in the background, gateway has a few-
        // second window after boot where it routes via the legacy DNS
        // path while cross may already be on etcd. A drift like that is
        // exactly the problem this watcher exists to fix. Synchronously
        // wait for the first snapshot inside StartAsync so by the time
        // gateway is "started" the ring matches every other pod's view.
        // 30 s cap so a permanently-down etcd doesn't block boot.
        var firstSnapshotCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        firstSnapshotCts.CancelAfter(TimeSpan.FromSeconds(30));

        try
        {
            _etcd = new EtcdClient(
                connectionString: _endpoint!,
                configureChannelOptions: opts =>
                {
                    opts.Credentials = Grpc.Core.ChannelCredentials.Insecure;
                });

            long startRevision = await SnapshotAndPublishAsync(firstSnapshotCts.Token);
            Console.WriteLine(
                $"[Gateway/EtcdMembershipWatcher] initial snapshot complete at revision {startRevision} " +
                $"({_members.Count} members)");

            _runner = Task.Run(() => RunWatchAsync(startRevision + 1, _cts.Token));
        }
        catch (OperationCanceledException) when (firstSnapshotCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            Console.WriteLine(
                "[Gateway/EtcdMembershipWatcher] initial snapshot timed out after 30s — " +
                "gateway will start with the legacy DNS path while the background watcher keeps retrying.");
            _runner = Task.Run(() => RunAsync(_cts.Token));
        }
        catch (Exception ex)
        {
            Console.WriteLine(
                $"[Gateway/EtcdMembershipWatcher] initial snapshot failed ({ex.GetType().Name}: {ex.Message}) — " +
                "gateway will start with the legacy DNS path while the background watcher keeps retrying.");
            _runner = Task.Run(() => RunAsync(_cts.Token));
        }
    }

    private async Task RunWatchAsync(long fromRevision, CancellationToken token)
    {
        try
        {
            if (_etcd == null) throw new InvalidOperationException("etcd client not initialised");
            var watchReq = new WatchRequest
            {
                CreateRequest = new WatchCreateRequest
                {
                    Key = ByteString.CopyFromUtf8("/agents/"),
                    RangeEnd = ByteString.CopyFromUtf8(PrefixEnd("/agents/")),
                    StartRevision = fromRevision,
                }
            };
            await _etcd.WatchAsync(watchReq, OnWatchEvent, cancellationToken: token);
            Console.WriteLine("[Gateway/EtcdMembershipWatcher] initial watch stream ended; switching to reconnect loop");
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { return; }
        catch (Exception ex)
        {
            Console.WriteLine($"[Gateway/EtcdMembershipWatcher] initial watch error: {ex.GetType().Name}: {ex.Message}");
        }

        if (!token.IsCancellationRequested)
            await RunAsync(token);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            _cts?.Cancel();
            if (_runner != null)
            {
                await Task.WhenAny(_runner, Task.Delay(TimeSpan.FromSeconds(3), cancellationToken));
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Gateway/EtcdMembershipWatcher] StopAsync error: {ex.Message}");
        }
        finally
        {
            try { _etcd?.Dispose(); } catch { /* best effort */ }
        }
    }

    private async Task RunAsync(CancellationToken token)
    {
        var backoff = TimeSpan.FromSeconds(1);
        var maxBackoff = TimeSpan.FromSeconds(15);

        while (!token.IsCancellationRequested)
        {
            try
            {
                _etcd?.Dispose();
                _etcd = new EtcdClient(
                    connectionString: _endpoint!,
                    configureChannelOptions: opts =>
                    {
                        opts.Credentials = Grpc.Core.ChannelCredentials.Insecure;
                    });

                long startRevision = await SnapshotAndPublishAsync(token);
                Console.WriteLine($"[Gateway/EtcdMembershipWatcher] initial snapshot complete at revision {startRevision}");

                backoff = TimeSpan.FromSeconds(1);

                var watchReq = new WatchRequest
                {
                    CreateRequest = new WatchCreateRequest
                    {
                        Key = ByteString.CopyFromUtf8("/agents/"),
                        RangeEnd = ByteString.CopyFromUtf8(PrefixEnd("/agents/")),
                        StartRevision = startRevision + 1,
                    }
                };

                await _etcd.WatchAsync(
                    watchReq,
                    OnWatchEvent,
                    cancellationToken: token);

                Console.WriteLine("[Gateway/EtcdMembershipWatcher] watch stream ended cleanly; restarting");
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Gateway/EtcdMembershipWatcher] watch error: {ex.GetType().Name}: {ex.Message}; retrying in {backoff.TotalSeconds}s");
            }

            try { await Task.Delay(backoff, token); }
            catch (OperationCanceledException) { return; }

            backoff = TimeSpan.FromTicks(Math.Min(maxBackoff.Ticks, backoff.Ticks * 2));
        }
    }

    private async Task<long> SnapshotAndPublishAsync(CancellationToken token)
    {
        if (_etcd == null) throw new InvalidOperationException("etcd client not initialised");

        var rangeReq = new RangeRequest
        {
            Key = ByteString.CopyFromUtf8("/agents/"),
            RangeEnd = ByteString.CopyFromUtf8(PrefixEnd("/agents/")),
        };

        var resp = await _etcd.GetAsync(rangeReq, cancellationToken: token);

        _members.Clear();
        _vnodesPerMember.Clear();
        foreach (var kv in resp.Kvs)
        {
            if (TryParseEntry(kv, out var name, out var ip, out var vnodes))
            {
                _members[name] = ip;
                _vnodesPerMember[name] = vnodes;
            }
        }

        PublishToRing();
        return resp.Header?.Revision ?? 0;
    }

    private void OnWatchEvent(WatchResponse response)
    {
        bool changed = false;
        foreach (var ev in response.Events)
        {
            switch (ev.Type)
            {
                case Event.Types.EventType.Put:
                    if (TryParseEntry(ev.Kv, out var name, out var ip, out var vnodes))
                    {
                        bool ipChanged = !_members.TryGetValue(name, out var prev) || prev != ip;
                        bool vnodesChanged = !_vnodesPerMember.TryGetValue(name, out var prevV) || prevV != vnodes;
                        if (ipChanged || vnodesChanged)
                        {
                            _members[name] = ip;
                            _vnodesPerMember[name] = vnodes;
                            changed = true;
                        }
                    }
                    break;

                case Event.Types.EventType.Delete:
                    if (TryGetKeyName(ev.Kv, out var delName))
                    {
                        bool removed = _members.Remove(delName);
                        _vnodesPerMember.Remove(delName);
                        if (removed) changed = true;
                    }
                    break;
            }
        }

        if (changed)
            PublishToRing();
    }

    private void PublishToRing()
    {
        var snapshot = new Dictionary<string, string>(_members);

        int vnodesPerAgent = ConsistentHashRing.DefaultVnodesPerAgent;
        if (_vnodesPerMember.Count > 0)
        {
            vnodesPerAgent = _vnodesPerMember.Values
                .GroupBy(v => v)
                .OrderByDescending(g => g.Count())
                .First()
                .Key;
        }
        var ring = ConsistentHashRing.Build(snapshot, vnodesPerAgent);
        bool changed = RingState.Set(ring);
        if (changed)
        {
            Console.WriteLine(
                $"[Gateway/EtcdMembershipWatcher] ring rebuilt: {ring.Agents.Count} agents, " +
                $"{vnodesPerAgent} vnodes/agent, topology version {RingState.TopologyVersion}");
        }
    }

    private static bool TryParseEntry(KeyValue kv, out string name, out string ip, out int vnodesPerAgent)
    {
        name = string.Empty;
        ip = string.Empty;
        vnodesPerAgent = ConsistentHashRing.DefaultVnodesPerAgent;
        if (!TryGetKeyName(kv, out name)) return false;

        try
        {
            var json = kv.Value.ToStringUtf8();
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            if (!root.TryGetProperty("ip", out var ipEl))
                return false;
            var parsed = ipEl.GetString();
            if (string.IsNullOrWhiteSpace(parsed))
                return false;
            ip = parsed!;

            if (root.TryGetProperty("vnodes", out var vnEl) && vnEl.ValueKind == JsonValueKind.Number)
            {
                if (vnEl.TryGetInt32(out var vn) && vn > 0) vnodesPerAgent = vn;
            }

            if (root.TryGetProperty("status", out var statusEl)
                && statusEl.ValueKind == JsonValueKind.String)
            {
                var status = statusEl.GetString();
                if (!string.IsNullOrEmpty(status) &&
                    !string.Equals(status, "ready", StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }
            }

            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Gateway/EtcdMembershipWatcher] failed to parse /agents/{name}: {ex.Message}");
        }

        return false;
    }

    private static bool TryGetKeyName(KeyValue kv, out string name)
    {
        name = string.Empty;
        var k = kv.Key.ToStringUtf8();
        const string prefix = "/agents/";
        if (!k.StartsWith(prefix, StringComparison.Ordinal)) return false;
        var rest = k.Substring(prefix.Length);
        if (string.IsNullOrWhiteSpace(rest)) return false;
        name = rest;
        return true;
    }

    private static string PrefixEnd(string prefix)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(prefix);
        if (bytes.Length == 0) return "\0";
        for (int i = bytes.Length - 1; i >= 0; i--)
        {
            if (bytes[i] < 0xFF)
            {
                bytes[i]++;
                return System.Text.Encoding.UTF8.GetString(bytes, 0, i + 1);
            }
        }
        return prefix + "\uFFFF";
    }
}
