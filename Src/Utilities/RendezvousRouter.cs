using System.Net;
using System.Text;
using Google.Protobuf.WellKnownTypes;

namespace Gateway.Utils;

/// <summary>
/// Rendezvous (Highest Random Weight) hashing.
/// Hashes by STABLE NODE NAMES (not pod IPs).
/// Pod IPs change on restart → routing breaks.
/// Node names are immutable → routing is stable across pod restarts.
/// </summary>
public static class RendezvousRouter
{
    private static readonly object _lock = new();

    // ── Stable routing identity (node names don't change across pod restarts) ──
    private static volatile string[] _nodeNames = Array.Empty<string>();
    private static volatile byte[][] _nodeNameBytes = Array.Empty<byte[]>();

    // ── Connection mapping (pod IPs may change, this gets refreshed) ──
    private static volatile Dictionary<string, string> _nodeToIp = new();

    // ── All current pod IPs ──
    private static volatile string[] _agentIps = Array.Empty<string>();

    private static long _resolvedAtTicks = 0;
    // When the last resolution succeeded fully (every IP returned a node name)
    // we cache for 15 s. When it succeeded only partially or fell all the way
    // back to IP-based routing we cache for a much shorter window so the next
    // PickAgent re-resolves and picks up agents as they finish warming up,
    // instead of locking the gateway into a stale fallback for the full 15 s.
    private static volatile bool _lastResolutionWasFallback = false;
    private static readonly TimeSpan FullCacheTtl = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan FallbackCacheTtl = TimeSpan.FromSeconds(2);

    public static string PickAgent(string bucketString)
    {
        var nodeNames = _nodeNames;
        if (nodeNames.Length == 0)
        {
            GetAgents();
            nodeNames = _nodeNames;
            if (nodeNames.Length == 0) return Gateway.Utils.Globals.Globals.AgentsLoadbalancer;
        }
        if (nodeNames.Length == 1)
        {
            var mapping = _nodeToIp;
            return mapping.TryGetValue(nodeNames[0], out var singleIp) ? singleIp : Gateway.Utils.Globals.Globals.AgentsLoadbalancer;
        }

        var nodeNameBytesLocal = _nodeNameBytes;
        int bucketLen = bucketString.Length;
        Span<byte> buf = stackalloc byte[192];

        for (int c = 0; c < bucketLen; c++)
            buf[c] = (byte)bucketString[c];
        buf[bucketLen] = (byte)'|';

        string bestNode = nodeNames[0];
        uint bestHash = 0;

        for (int i = 0; i < nodeNames.Length; i++)
        {
            var nb = nodeNameBytesLocal[i];
            nb.AsSpan().CopyTo(buf.Slice(bucketLen + 1));
            int totalLen = bucketLen + 1 + nb.Length;

            uint hash = MurmurHash3(buf.Slice(0, totalLen));
            if (hash > bestHash)
            {
                bestHash = hash;
                bestNode = nodeNames[i];
            }
        }

        var nodeMapping = _nodeToIp;
        return nodeMapping.TryGetValue(bestNode, out var agentIp) ? agentIp : Gateway.Utils.Globals.Globals.AgentsLoadbalancer;
    }

    public static string[] GetAgents()
    {
        long resolvedTicks = Interlocked.Read(ref _resolvedAtTicks);
        var cacheTtl = _lastResolutionWasFallback ? FallbackCacheTtl : FullCacheTtl;
        if (_nodeNames.Length > 0 && (DateTime.UtcNow.Ticks - resolvedTicks) < cacheTtl.Ticks)
            return _agentIps;

        lock (_lock)
        {
            cacheTtl = _lastResolutionWasFallback ? FallbackCacheTtl : FullCacheTtl;
            if (_nodeNames.Length > 0 && (DateTime.UtcNow.Ticks - _resolvedAtTicks) < cacheTtl.Ticks)
                return _agentIps;

            try
            {
                var resolvedIps = Dns.GetHostAddresses(Gateway.Utils.Globals.Globals.AgentsLoadbalancer)
                    .Where(ip => ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                    .Select(ip => ip.ToString())
                    .Distinct()
                    .ToArray();

                if (resolvedIps.Length == 0)
                    return _agentIps.Length > 0 ? _agentIps : new[] { Gateway.Utils.Globals.Globals.AgentsLoadbalancer };

                var newNodeToIp = new Dictionary<string, string>();
                var tasks = resolvedIps.Select(async ip =>
                {
                    try
                    {
                        var client = GrpcChannelFactory.GetClient(
                            target: ip,
                            ctor: chan => new GetNodeInfo.GetNodeInfoClient(chan),
                            roundRobin: false,
                            port: 5000);

                        // Was 3 s. Tight enough that any agent doing warm-up
                        // work (RocksDB scan, bucket cache priming, GC stall
                        // during boot) blew through it and got marked
                        // unresolvable, dragging the gateway into the
                        // IP-based fallback for 15 s. 8 s is well above the
                        // p99 healthy GetNodeInfo while still bounded by the
                        // outer Task.WaitAll.
                        var res = await client.GetAsync(new Empty(),
                            deadline: DateTime.UtcNow.AddSeconds(8));

                        string nodeName = res.NodeName;
                        if (!string.IsNullOrWhiteSpace(nodeName))
                            return (nodeName, ip, ok: true);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[RendezvousRouter] GetNodeInfo failed for {ip}: {ex.Message}");
                    }
                    return (nodeName: "", ip, ok: false);
                }).ToArray();

                // Outer wait must exceed the per-call deadline so a slow
                // agent isn't silently dropped by the WaitAll racing the
                // deadline. 12 s leaves a small buffer above 8 s.
                Task.WaitAll(tasks, TimeSpan.FromSeconds(12));

                foreach (var t in tasks)
                {
                    if (t.IsCompletedSuccessfully && t.Result.ok)
                        newNodeToIp[t.Result.nodeName] = t.Result.ip;
                }

                if (newNodeToIp.Count == 0)
                {
                    // Fallback to IP-based routing (agents might not have GetNodeInfo yet).
                    // Marked as fallback so the cache TTL drops to FallbackCacheTtl —
                    // we want to re-probe agents as soon as the next request arrives
                    // so routing repairs itself within seconds, not 15 s.
                    Console.WriteLine($"[RendezvousRouter] WARNING: GetNodeInfo failed for all agents. Falling back to IP-based routing (will re-probe in {FallbackCacheTtl.TotalSeconds}s).");
                    var sortedIps = resolvedIps.OrderBy(x => x, StringComparer.Ordinal).ToArray();
                    _nodeNames = sortedIps;
                    _nodeNameBytes = sortedIps.Select(a => Encoding.UTF8.GetBytes(a)).ToArray();
                    _nodeToIp = sortedIps.ToDictionary(ip => ip, ip => ip);
                    _agentIps = sortedIps;
                    _lastResolutionWasFallback = true;
                    Interlocked.Exchange(ref _resolvedAtTicks, DateTime.UtcNow.Ticks);
                    return _agentIps;
                }

                var sortedNodeNames = newNodeToIp.Keys.OrderBy(x => x, StringComparer.Ordinal).ToArray();
                bool changed = !sortedNodeNames.SequenceEqual(_nodeNames);

                _nodeNameBytes = sortedNodeNames.Select(n => Encoding.UTF8.GetBytes(n)).ToArray();
                _nodeNames = sortedNodeNames;
                _nodeToIp = newNodeToIp;
                _agentIps = newNodeToIp.Values.ToArray();
                // A "partial" success (some agents responded, others didn't) is also
                // treated as a fallback so we re-probe quickly and pick up the rest as
                // they become responsive, keeping rendezvous routing complete.
                _lastResolutionWasFallback = newNodeToIp.Count != resolvedIps.Length;
                Interlocked.Exchange(ref _resolvedAtTicks, DateTime.UtcNow.Ticks);

                if (changed)
                {
                    var info = string.Join(", ", sortedNodeNames.Select(n => $"{n}={newNodeToIp[n]}"));
                    Console.WriteLine($"[RendezvousRouter] Resolved {sortedNodeNames.Length} agents by node name: [{info}]");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[RendezvousRouter] DNS resolve failed: {ex.Message}");
            }

            return _agentIps.Length > 0 ? _agentIps : new[] { Gateway.Utils.Globals.Globals.AgentsLoadbalancer };
        }
    }

    private static uint MurmurHash3(ReadOnlySpan<byte> bytes)
    {
        const uint seed = 0x9747b28c;
        const uint c1 = 0xcc9e2d51;
        const uint c2 = 0x1b873593;

        uint hash = seed;
        int length = bytes.Length;
        int nblocks = length / 4;

        for (int i = 0; i < nblocks; i++)
        {
            uint k = BitConverter.ToUInt32(bytes.Slice(i * 4, 4));
            k *= c1; k = RotateLeft(k, 15); k *= c2;
            hash ^= k; hash = RotateLeft(hash, 13); hash = hash * 5 + 0xe6546b64;
        }

        uint tail = 0;
        int tailStart = nblocks * 4;
        switch (length & 3)
        {
            case 3: tail ^= (uint)bytes[tailStart + 2] << 16; goto case 2;
            case 2: tail ^= (uint)bytes[tailStart + 1] << 8; goto case 1;
            case 1: tail ^= bytes[tailStart]; tail *= c1; tail = RotateLeft(tail, 15); tail *= c2; hash ^= tail; break;
        }

        hash ^= (uint)length;
        hash ^= hash >> 16; hash *= 0x85ebca6b;
        hash ^= hash >> 13; hash *= 0xc2b2ae35;
        hash ^= hash >> 16;
        return hash;
    }

    private static uint RotateLeft(uint x, int r) => (x << r) | (x >> (32 - r));
}
