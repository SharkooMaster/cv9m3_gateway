using System.Net;
using System.Text;
using Google.Protobuf.WellKnownTypes;

namespace Gateway.Utils;

/// <summary>
/// Rendezvous (Highest Random Weight) hashing — uses STABLE Kubernetes node names
/// for the hash key, NOT ephemeral pod IPs.
///
/// Node names (spec.nodeName) never change across pod restarts / helm upgrades.
/// Same node → same data (hostPath) → same routing → no corruption.
/// </summary>
public static class RendezvousRouter
{
    private static readonly object _lock = new();

    // ── Stable routing data ──
    private static string[] _nodeNames = Array.Empty<string>();
    private static byte[][] _nodeNameBytes = Array.Empty<byte[]>();
    private static string[] _nodeIps = Array.Empty<string>();
    private static DateTime _resolvedAt = DateTime.MinValue;

    /// <summary>
    /// Pick the owning agent for a bucket using rendezvous hashing.
    /// Deterministic: same (bucket, node set) always returns the same agent IP.
    /// </summary>
    public static string PickAgent(string bucketString)
    {
        var nodeNames = _nodeNames;
        var nodeIps = _nodeIps;

        if (nodeNames.Length == 0)
        {
            GetAgents();
            nodeNames = _nodeNames;
            nodeIps = _nodeIps;
            if (nodeNames.Length == 0) return Gateway.Utils.Globals.Globals.AgentsLoadbalancer;
        }
        if (nodeNames.Length == 1) return nodeIps[0];

        var nodeNameBytesLocal = _nodeNameBytes;

        int bucketLen = bucketString.Length;
        Span<byte> buf = stackalloc byte[160];

        for (int c = 0; c < bucketLen; c++)
            buf[c] = (byte)bucketString[c];
        buf[bucketLen] = (byte)'|';

        string bestIp = nodeIps[0];
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
                bestIp = nodeIps[i];
            }
        }
        return bestIp;
    }

    /// <summary>
    /// Resolve agents: DNS → pod IPs → GetNodeInfo gRPC → node names.
    /// Cached 15s.
    /// </summary>
    public static string[] GetAgents()
    {
        lock (_lock)
        {
            if (_nodeNames.Length > 0 && DateTime.UtcNow - _resolvedAt < TimeSpan.FromSeconds(15))
                return _nodeIps;
        }

        try
        {
            var podIps = Dns.GetHostAddresses(Gateway.Utils.Globals.Globals.AgentsLoadbalancer)
                .Where(ip => ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                .Select(ip => ip.ToString())
                .Distinct()
                .OrderBy(x => x)
                .ToArray();

            if (podIps.Length == 0)
            {
                lock (_lock) { return _nodeIps.Length > 0 ? _nodeIps : new[] { Gateway.Utils.Globals.Globals.AgentsLoadbalancer }; }
            }

            var entries = new List<(string nodeName, string podIp)>(podIps.Length);
            var tasks = podIps.Select(async ip =>
            {
                try
                {
                    var client = GrpcChannelFactory.GetClient(
                        target: ip,
                        ctor: chan => new GetNodeInfo.GetNodeInfoClient(chan),
                        roundRobin: false,
                        port: 5000);

                    var res = await client.GetAsync(
                        new Empty(),
                        deadline: DateTime.UtcNow.AddSeconds(3));

                    string nodeName = res.NodeName;
                    if (string.IsNullOrWhiteSpace(nodeName))
                        nodeName = ip;

                    return (nodeName, ip);
                }
                catch
                {
                    return (ip, ip);
                }
            }).ToArray();

            Task.WaitAll(tasks);
            foreach (var t in tasks)
                entries.Add(t.Result);

            entries.Sort((a, b) => string.Compare(a.nodeName, b.nodeName, StringComparison.Ordinal));

            var newNodeNames = entries.Select(e => e.nodeName).ToArray();
            var newNodeIps = entries.Select(e => e.podIp).ToArray();
            var newNodeNameBytes = newNodeNames.Select(n => Encoding.UTF8.GetBytes(n)).ToArray();

            lock (_lock)
            {
                bool changed = !newNodeNames.SequenceEqual(_nodeNames) || !newNodeIps.SequenceEqual(_nodeIps);
                _nodeNameBytes = newNodeNameBytes;
                _nodeNames = newNodeNames;
                _nodeIps = newNodeIps;
                _resolvedAt = DateTime.UtcNow;

                if (changed)
                {
                    var pairs = entries.Select(e => e.nodeName == e.podIp
                        ? e.podIp
                        : $"{e.nodeName}={e.podIp}");
                    Console.WriteLine($"[RendezvousRouter] Resolved {entries.Count} agents: [{string.Join(", ", pairs)}]");
                }
            }
        }
        catch
        {
            // Keep previous resolution on failure
        }

        lock (_lock)
        {
            return _nodeIps.Length > 0 ? _nodeIps : new[] { Gateway.Utils.Globals.Globals.AgentsLoadbalancer };
        }
    }

    /// <summary>
    /// MurmurHash3 32-bit — Span overload, zero allocation.
    /// </summary>
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
            k *= c1;
            k = RotateLeft(k, 15);
            k *= c2;
            hash ^= k;
            hash = RotateLeft(hash, 13);
            hash = hash * 5 + 0xe6546b64;
        }

        uint tail = 0;
        int tailStart = nblocks * 4;
        switch (length & 3)
        {
            case 3: tail ^= (uint)bytes[tailStart + 2] << 16; goto case 2;
            case 2: tail ^= (uint)bytes[tailStart + 1] << 8; goto case 1;
            case 1:
                tail ^= bytes[tailStart];
                tail *= c1;
                tail = RotateLeft(tail, 15);
                tail *= c2;
                hash ^= tail;
                break;
        }

        hash ^= (uint)length;
        hash ^= hash >> 16;
        hash *= 0x85ebca6b;
        hash ^= hash >> 13;
        hash *= 0xc2b2ae35;
        hash ^= hash >> 16;
        return hash;
    }

    private static uint RotateLeft(uint x, int r) => (x << r) | (x >> (32 - r));
}
