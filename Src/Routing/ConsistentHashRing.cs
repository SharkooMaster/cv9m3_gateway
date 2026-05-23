using System.Text;

namespace Gateway.Routing;

/// <summary>
/// Consistent-hash ring with virtual nodes for replicated agent routing.
///
/// Mirror of cross/src/Routing/ConsistentHashRing.cs. C# can't share a
/// single source file across two .csproj projects without a third shared
/// project — keeping a verbatim copy here is simpler and the algorithm
/// is small. Any change to one MUST be mirrored to the other; the test
/// suite enforces hash agreement between the two on common inputs.
/// </summary>
public sealed class ConsistentHashRing
{
    public const int DefaultVnodesPerAgent = 256;

    private readonly RingEntry[] _entries;
    private readonly Dictionary<string, string> _agentToIp;

    public IReadOnlyList<string> Agents { get; }
    public int VnodesPerAgent { get; }

    public static readonly ConsistentHashRing Empty = new(
        Array.Empty<RingEntry>(),
        new Dictionary<string, string>(),
        Array.Empty<string>(),
        DefaultVnodesPerAgent);

    private ConsistentHashRing(
        RingEntry[] entries,
        Dictionary<string, string> agentToIp,
        IReadOnlyList<string> agents,
        int vnodesPerAgent)
    {
        _entries = entries;
        _agentToIp = agentToIp;
        Agents = agents;
        VnodesPerAgent = vnodesPerAgent;
    }

    public static ConsistentHashRing Build(
        IReadOnlyDictionary<string, string> agentToIp,
        int vnodesPerAgent = DefaultVnodesPerAgent)
    {
        if (agentToIp == null) throw new ArgumentNullException(nameof(agentToIp));
        if (vnodesPerAgent <= 0)
            throw new ArgumentOutOfRangeException(nameof(vnodesPerAgent), "vnodesPerAgent must be > 0");

        if (agentToIp.Count == 0)
            return new ConsistentHashRing(
                Array.Empty<RingEntry>(),
                new Dictionary<string, string>(),
                Array.Empty<string>(),
                vnodesPerAgent);

        var sortedAgents = agentToIp.Keys.OrderBy(x => x, StringComparer.Ordinal).ToArray();

        var entries = new RingEntry[sortedAgents.Length * vnodesPerAgent];
        Span<byte> buf = stackalloc byte[256];
        int idx = 0;
        for (int a = 0; a < sortedAgents.Length; a++)
        {
            var name = sortedAgents[a];
            int nameLen = Encoding.UTF8.GetBytes(name, buf);
            buf[nameLen] = (byte)'#';

            for (int v = 0; v < vnodesPerAgent; v++)
            {
                int vlen = WriteIntDecimal(v, buf.Slice(nameLen + 1));
                int totalLen = nameLen + 1 + vlen;
                uint hash = MurmurHash3.Hash(buf.Slice(0, totalLen));
                entries[idx++] = new RingEntry(hash, a);
            }
        }

        Array.Sort(entries, RingEntryComparer.Instance);

        var agentToIpCopy = new Dictionary<string, string>(agentToIp.Count);
        foreach (var kv in agentToIp) agentToIpCopy[kv.Key] = kv.Value;

        return new ConsistentHashRing(entries, agentToIpCopy, sortedAgents, vnodesPerAgent);
    }

    public IReadOnlyList<string> PickReplicas(string key, int replicaCount)
    {
        if (replicaCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(replicaCount), "replicaCount must be > 0");
        if (_entries.Length == 0 || Agents.Count == 0)
            return Array.Empty<string>();

        int targetCount = Math.Min(replicaCount, Agents.Count);
        var result = new List<string>(targetCount);
        var seen = new HashSet<int>(targetCount);

        uint keyHash = MurmurHash3.Hash(Encoding.UTF8.GetBytes(key));
        int start = LowerBound(keyHash);

        int ringLen = _entries.Length;
        for (int i = 0; i < ringLen && result.Count < targetCount; i++)
        {
            int slot = (start + i) % ringLen;
            int agentIdx = _entries[slot].AgentIndex;
            if (seen.Add(agentIdx))
            {
                result.Add(Agents[agentIdx]);
            }
        }

        return result;
    }

    public IReadOnlyList<string> PickReplicaIps(string key, int replicaCount)
    {
        var names = PickReplicas(key, replicaCount);
        if (names.Count == 0) return Array.Empty<string>();

        var ips = new List<string>(names.Count);
        foreach (var name in names)
        {
            if (_agentToIp.TryGetValue(name, out var ip) && !string.IsNullOrEmpty(ip))
                ips.Add(ip);
        }
        return ips;
    }

    public string? PickPrimary(string key)
    {
        var replicas = PickReplicas(key, 1);
        return replicas.Count > 0 ? replicas[0] : null;
    }

    public string? GetIp(string agentName)
    {
        return _agentToIp.TryGetValue(agentName, out var ip) ? ip : null;
    }

    private int LowerBound(uint keyHash)
    {
        int lo = 0, hi = _entries.Length;
        while (lo < hi)
        {
            int mid = (lo + hi) >>> 1;
            if (_entries[mid].Hash < keyHash) lo = mid + 1;
            else hi = mid;
        }
        return lo == _entries.Length ? 0 : lo;
    }

    private static int WriteIntDecimal(int value, Span<byte> dst)
    {
        if (value == 0)
        {
            dst[0] = (byte)'0';
            return 1;
        }
        int n = value;
        int len = 0;
        Span<byte> tmp = stackalloc byte[12];
        while (n > 0)
        {
            tmp[len++] = (byte)('0' + (n % 10));
            n /= 10;
        }
        for (int i = 0; i < len; i++) dst[i] = tmp[len - 1 - i];
        return len;
    }

    private readonly struct RingEntry
    {
        public readonly uint Hash;
        public readonly int AgentIndex;
        public RingEntry(uint hash, int agentIndex) { Hash = hash; AgentIndex = agentIndex; }
    }

    private sealed class RingEntryComparer : IComparer<RingEntry>
    {
        public static readonly RingEntryComparer Instance = new();
        public int Compare(RingEntry a, RingEntry b) => a.Hash.CompareTo(b.Hash);
    }
}

internal static class MurmurHash3
{
    public static uint Hash(ReadOnlySpan<byte> bytes)
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
