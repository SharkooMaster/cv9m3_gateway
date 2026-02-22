using System.Net;
using System.Text;

namespace Gateway.Utils;

/// <summary>
/// Rendezvous (Highest Random Weight) hashing.
/// Hash by sorted agent IPs from DNS. Consistent across all pods.
/// </summary>
public static class RendezvousRouter
{
    private static readonly object _lock = new();
    private static string[] _agents = Array.Empty<string>();
    private static byte[][] _agentBytes = Array.Empty<byte[]>();
    private static DateTime _resolvedAt = DateTime.MinValue;

    public static string PickAgent(string bucketString)
    {
        var agents = _agents;
        if (agents.Length == 0)
        {
            GetAgents();
            agents = _agents;
            if (agents.Length == 0) return Gateway.Utils.Globals.Globals.AgentsLoadbalancer;
        }
        if (agents.Length == 1) return agents[0];

        var agentBytesLocal = _agentBytes;
        int bucketLen = bucketString.Length;
        Span<byte> buf = stackalloc byte[128];

        for (int c = 0; c < bucketLen; c++)
            buf[c] = (byte)bucketString[c];
        buf[bucketLen] = (byte)'|';

        string bestAgent = agents[0];
        uint bestHash = 0;

        for (int i = 0; i < agents.Length; i++)
        {
            var ab = agentBytesLocal[i];
            ab.AsSpan().CopyTo(buf.Slice(bucketLen + 1));
            int totalLen = bucketLen + 1 + ab.Length;

            uint hash = MurmurHash3(buf.Slice(0, totalLen));
            if (hash > bestHash)
            {
                bestHash = hash;
                bestAgent = agents[i];
            }
        }
        return bestAgent;
    }

    public static string[] GetAgents()
    {
        lock (_lock)
        {
            if (_agents.Length > 0 && DateTime.UtcNow - _resolvedAt < TimeSpan.FromSeconds(60))
                return _agents;
        }

        try
        {
            var resolved = Dns.GetHostAddresses(Gateway.Utils.Globals.Globals.AgentsLoadbalancer)
                .Where(ip => ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                .Select(ip => ip.ToString())
                .Distinct()
                .OrderBy(x => x, StringComparer.Ordinal)
                .ToArray();

            if (resolved.Length > 0)
            {
                lock (_lock)
                {
                    bool changed = !resolved.SequenceEqual(_agents);
                    _agentBytes = resolved.Select(a => Encoding.UTF8.GetBytes(a)).ToArray();
                    _agents = resolved;
                    _resolvedAt = DateTime.UtcNow;

                    if (changed)
                        Console.WriteLine($"[RendezvousRouter] Resolved {resolved.Length} agents: [{string.Join(", ", resolved)}]");
                }
            }
        }
        catch { }

        lock (_lock)
        {
            return _agents.Length > 0 ? _agents : new[] { Gateway.Utils.Globals.Globals.AgentsLoadbalancer };
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
