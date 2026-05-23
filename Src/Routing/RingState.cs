namespace Gateway.Routing;

/// <summary>
/// Process-wide holder for the active <see cref="ConsistentHashRing"/>.
/// Mirror of cross/src/Routing/RingState.cs — see that file for the full
/// rationale (single immutable snapshot, atomic swap on each etcd event,
/// monotonic topology version).
/// </summary>
public static class RingState
{
    private static volatile ConsistentHashRing _current = ConsistentHashRing.Empty;
    private static long _topologyVersion;
    private static long _lastUpdatedTicks;

    public static ConsistentHashRing Current => _current;
    public static long TopologyVersion => Interlocked.Read(ref _topologyVersion);

    public static DateTime LastUpdated
    {
        get
        {
            long t = Interlocked.Read(ref _lastUpdatedTicks);
            return t == 0 ? DateTime.MinValue : new DateTime(t, DateTimeKind.Utc);
        }
    }

    public static bool Set(ConsistentHashRing next)
    {
        if (next == null) throw new ArgumentNullException(nameof(next));

        var prev = _current;
        bool changed = !RingsAreEqual(prev, next);
        _current = next;
        Interlocked.Exchange(ref _lastUpdatedTicks, DateTime.UtcNow.Ticks);
        if (changed)
        {
            Interlocked.Increment(ref _topologyVersion);
        }
        return changed;
    }

    private static bool RingsAreEqual(ConsistentHashRing a, ConsistentHashRing b)
    {
        if (ReferenceEquals(a, b)) return true;
        if (a.Agents.Count != b.Agents.Count) return false;
        if (a.VnodesPerAgent != b.VnodesPerAgent) return false;
        for (int i = 0; i < a.Agents.Count; i++)
        {
            if (!string.Equals(a.Agents[i], b.Agents[i], StringComparison.Ordinal)) return false;
            var aIp = a.GetIp(a.Agents[i]);
            var bIp = b.GetIp(b.Agents[i]);
            if (!string.Equals(aIp, bIp, StringComparison.Ordinal)) return false;
        }
        return true;
    }
}
