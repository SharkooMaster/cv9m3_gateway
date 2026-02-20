using System.Net;
using System.Text;
namespace Gateway.Utils;

/// <summary>
/// Rendezvous (Highest Random Weight) hashing for deterministic bucket-to-agent routing.
/// For N agents, only ~1/N of buckets remap when a node joins/leaves.
/// Zero network overhead — every node can compute the answer independently.
/// </summary>
public static class RendezvousRouter
{
    private static readonly object _lock = new();
    private static string[] _agents = Array.Empty<string>();
    private static DateTime _resolvedAt = DateTime.MinValue;

    /// <summary>
    /// Pick the owning agent for a bucket using rendezvous hashing.
    /// Deterministic: same (bucket, agent set) always returns the same agent.
    /// O(N) where N = agent count. For 5-20 agents this is ~nanoseconds.
    /// </summary>
    public static string PickAgent(string bucketString)
    {
        var agents = GetAgents();
        if (agents.Length == 0) return Gateway.Utils.Globals.Globals.AgentsLoadbalancer;
        if (agents.Length == 1) return agents[0];

        string bestAgent = agents[0];
        uint bestHash = 0;
        for (int i = 0; i < agents.Length; i++)
        {
            uint hash = MurmurHash3($"{bucketString}|{agents[i]}");
            if (hash > bestHash)
            {
                bestHash = hash;
                bestAgent = agents[i];
            }
        }
        return bestAgent;
    }

    /// <summary>
    /// Get the current set of known agent IPs. Resolves from DNS headless service, cached 15s.
    /// Sorted for deterministic ordering across all gateway instances.
    /// </summary>
    public static string[] GetAgents()
    {
        lock (_lock)
        {
            if (_agents.Length > 0 && DateTime.UtcNow - _resolvedAt < TimeSpan.FromSeconds(15))
                return _agents;
        }

        try
        {
            var resolved = Dns.GetHostAddresses(Gateway.Utils.Globals.Globals.AgentsLoadbalancer)
                .Where(ip => ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                .Select(ip => ip.ToString())
                .Distinct()
                .OrderBy(x => x) // Stable order across all callers
                .ToArray();

            if (resolved.Length > 0)
            {
                lock (_lock)
                {
                    _agents = resolved;
                    _resolvedAt = DateTime.UtcNow;
                }
            }
        }
        catch
        {
            // Keep previous resolution on DNS failure — transient failures shouldn't collapse routing
        }

        lock (_lock)
        {
            return _agents.Length > 0 ? _agents : new[] { Gateway.Utils.Globals.Globals.AgentsLoadbalancer };
        }
    }

    /// <summary>
    /// MurmurHash3 32-bit — fast, well-distributed, deterministic.
    /// </summary>
    private static uint MurmurHash3(string key)
    {
        var bytes = Encoding.UTF8.GetBytes(key);
        const uint seed = 0x9747b28c;
        const uint c1 = 0xcc9e2d51;
        const uint c2 = 0x1b873593;

        uint hash = seed;
        int length = bytes.Length;
        int nblocks = length / 4;

        for (int i = 0; i < nblocks; i++)
        {
            uint k = BitConverter.ToUInt32(bytes, i * 4);
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
