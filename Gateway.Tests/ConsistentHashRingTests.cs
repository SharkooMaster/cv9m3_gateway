using Gateway.Routing;

namespace Gateway.Tests;

/// <summary>
/// Unit tests for the consistent-hash ring. The properties tested here are
/// load-bearing for replication correctness:
///   - Determinism: every pod must produce the same ring on the same input
///     (or replicated reads/writes route to different agents from different
///     pods → the very bug we're trying to kill).
///   - Distinct replicas: PickReplicas(key, R) must return R *different*
///     agents (replication factor R=3 with 2 copies on the same pod is
///     not replication).
///   - Bounded movement on resize: adding an agent should re-point ~1/N
///     of the keys, not all of them. This is the "scaling without data
///     movement pain" property.
///   - Reasonable load distribution: vnodes must spread keys across
///     agents within ~10% of uniform.
/// </summary>
[Trait("Category", "Unit")]
public class ConsistentHashRingTests
{
    private static IReadOnlyDictionary<string, string> Membership(params string[] names)
    {
        var d = new Dictionary<string, string>();
        for (int i = 0; i < names.Length; i++) d[names[i]] = $"10.0.0.{i + 1}";
        return d;
    }

    [Fact]
    public void Empty_membership_returns_empty_replicas()
    {
        var ring = ConsistentHashRing.Build(new Dictionary<string, string>());
        Assert.Empty(ring.PickReplicas("any-key", 3));
    }

    [Fact]
    public void Single_agent_returns_only_that_agent_for_any_R()
    {
        var ring = ConsistentHashRing.Build(Membership("agent-0"));
        var picks = ring.PickReplicas("any-key", 3);
        Assert.Single(picks);
        Assert.Equal("agent-0", picks[0]);
    }

    [Fact]
    public void Replicas_are_distinct()
    {
        var ring = ConsistentHashRing.Build(Membership("a", "b", "c", "d", "e"));
        for (int i = 0; i < 1000; i++)
        {
            var picks = ring.PickReplicas($"key-{i}", 3);
            Assert.Equal(3, picks.Count);
            Assert.Equal(3, picks.Distinct().Count());
        }
    }

    [Fact]
    public void Picks_are_deterministic_across_independent_builds()
    {
        // The watcher rebuilds the ring on every etcd event from a fresh
        // dictionary. Two pods that build from the same map MUST produce
        // identical rings, otherwise replicated writes from cross pod A
        // and reads from cross pod B will diverge.
        var members = Membership("agent-0", "agent-1", "agent-2", "agent-3", "agent-4");
        var r1 = ConsistentHashRing.Build(members);
        var r2 = ConsistentHashRing.Build(members);

        for (int i = 0; i < 1000; i++)
        {
            var k = $"chunk-{i:x8}";
            var p1 = r1.PickReplicas(k, 3);
            var p2 = r2.PickReplicas(k, 3);
            Assert.Equal(p1, p2);
        }
    }

    [Fact]
    public void Determinism_is_independent_of_dictionary_insertion_order()
    {
        // Dictionary<,> iteration order is insertion-order in .NET; the
        // ring must sort agents internally so the input order doesn't
        // matter. Otherwise pods that built from a different etcd watch
        // event order would silently disagree.
        var forward = new Dictionary<string, string>
        {
            ["agent-0"] = "10.0.0.1",
            ["agent-1"] = "10.0.0.2",
            ["agent-2"] = "10.0.0.3",
            ["agent-3"] = "10.0.0.4",
        };
        var reverse = new Dictionary<string, string>
        {
            ["agent-3"] = "10.0.0.4",
            ["agent-2"] = "10.0.0.3",
            ["agent-1"] = "10.0.0.2",
            ["agent-0"] = "10.0.0.1",
        };

        var r1 = ConsistentHashRing.Build(forward);
        var r2 = ConsistentHashRing.Build(reverse);

        for (int i = 0; i < 500; i++)
        {
            var k = $"chunk-{i}";
            Assert.Equal(r1.PickReplicas(k, 3), r2.PickReplicas(k, 3));
        }
    }

    [Fact]
    public void Adding_agent_moves_bounded_fraction_of_keys()
    {
        // The point of vnodes: adding a node should remap roughly 1/N of
        // the keyspace, not invalidate the whole ring. We tolerate up to
        // 2x the ideal fraction to allow for variance with finite vnodes.
        var before = ConsistentHashRing.Build(Membership("a", "b", "c", "d"));
        var after = ConsistentHashRing.Build(Membership("a", "b", "c", "d", "e"));

        const int N = 10_000;
        int moved = 0;
        for (int i = 0; i < N; i++)
        {
            var k = $"chunk-{i}";
            var primaryBefore = before.PickPrimary(k);
            var primaryAfter = after.PickPrimary(k);
            if (primaryBefore != primaryAfter) moved++;
        }
        double fraction = (double)moved / N;
        // Ideal: 1/5 = 0.20 (because we went 4 → 5 agents and one of every
        // five keys should now point at the new agent).
        Assert.InRange(fraction, 0.10, 0.40);
    }

    [Fact]
    public void Removing_agent_moves_bounded_fraction_of_keys()
    {
        // Symmetry of the resize property: removing an agent should only
        // re-point its share of the keyspace, not the whole ring.
        var before = ConsistentHashRing.Build(Membership("a", "b", "c", "d", "e"));
        var after = ConsistentHashRing.Build(Membership("a", "b", "c", "d"));

        const int N = 10_000;
        int moved = 0;
        for (int i = 0; i < N; i++)
        {
            var k = $"chunk-{i}";
            var primaryBefore = before.PickPrimary(k);
            var primaryAfter = after.PickPrimary(k);
            if (primaryBefore != primaryAfter) moved++;
        }
        double fraction = (double)moved / N;
        // Ideal: 1/5 = 0.20 (the removed agent owned ~20% of keys).
        Assert.InRange(fraction, 0.10, 0.40);
    }

    [Fact]
    public void Load_is_uniform_within_10pct()
    {
        // With 256 vnodes per agent the standard deviation of per-agent
        // load over a large sample should be well under 10% of the mean.
        // This is what makes "more agents = more capacity" actually true.
        var ring = ConsistentHashRing.Build(Membership("a", "b", "c", "d", "e"));

        const int N = 50_000;
        var counts = new Dictionary<string, int>();
        foreach (var name in ring.Agents) counts[name] = 0;

        for (int i = 0; i < N; i++)
        {
            var primary = ring.PickPrimary($"chunk-{i:x8}");
            Assert.NotNull(primary);
            counts[primary!]++;
        }

        double mean = (double)N / ring.Agents.Count;
        foreach (var (name, c) in counts)
        {
            double dev = Math.Abs(c - mean) / mean;
            Assert.True(dev < 0.10, $"agent {name}: count={c}, deviation={dev:P2}");
        }
    }

    [Fact]
    public void Asking_for_more_replicas_than_agents_returns_what_is_available()
    {
        var ring = ConsistentHashRing.Build(Membership("a", "b"));
        var picks = ring.PickReplicas("any-key", 5);
        Assert.Equal(2, picks.Count);
        Assert.Contains("a", picks);
        Assert.Contains("b", picks);
    }

    [Fact]
    public void PickReplicaIps_translates_pod_names_to_pod_ips_in_order()
    {
        var ring = ConsistentHashRing.Build(Membership("a", "b", "c"));
        for (int i = 0; i < 100; i++)
        {
            var key = $"k-{i}";
            var names = ring.PickReplicas(key, 3);
            var ips = ring.PickReplicaIps(key, 3);
            Assert.Equal(3, ips.Count);
            for (int j = 0; j < 3; j++)
            {
                Assert.Equal(ring.GetIp(names[j]), ips[j]);
            }
        }
    }

    [Fact]
    public void Build_rejects_invalid_vnodes_per_agent()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            ConsistentHashRing.Build(Membership("a"), vnodesPerAgent: 0));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            ConsistentHashRing.Build(Membership("a"), vnodesPerAgent: -1));
    }

    [Fact]
    public void Pick_rejects_invalid_replica_count()
    {
        var ring = ConsistentHashRing.Build(Membership("a", "b"));
        Assert.Throws<ArgumentOutOfRangeException>(() => ring.PickReplicas("k", 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => ring.PickReplicas("k", -1));
    }

    [Fact]
    public void RingState_topology_version_only_bumps_on_real_change()
    {
        var initial = RingState.TopologyVersion;
        var r1 = ConsistentHashRing.Build(Membership("a", "b", "c"));
        Assert.True(RingState.Set(r1));
        Assert.Equal(initial + 1, RingState.TopologyVersion);

        // Identical membership rebuilt: ring is byte-for-byte equal so
        // version must NOT bump. Otherwise every spurious watcher event
        // would trigger a no-op rebalance.
        var r2 = ConsistentHashRing.Build(Membership("a", "b", "c"));
        Assert.False(RingState.Set(r2));
        Assert.Equal(initial + 1, RingState.TopologyVersion);

        // Add an agent: version bumps.
        var r3 = ConsistentHashRing.Build(Membership("a", "b", "c", "d"));
        Assert.True(RingState.Set(r3));
        Assert.Equal(initial + 2, RingState.TopologyVersion);
    }
}
