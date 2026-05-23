using System.Collections.Concurrent;

namespace Gateway.Services;

/// <summary>
/// Tracks in-flight handoff adoptions per leaving agent so the
/// agent-side <c>/admin/retire</c> blocking poll can decide when it's
/// safe for the pod to terminate.
///
/// Lifecycle:
///   1. <see cref="RebalanceCoordinator"/> sees an agent leave the ring
///      (typically because the agent itself flipped its etcd status to
///      "draining" via <c>POST /admin/retire</c>) and dispatches one
///      adoption per surviving agent, each pulling from the leaving pod.
///   2. Each <c>BeginVnodeAdoption</c> call returns an adoption_id; the
///      coordinator records (leaving pod name, adoption_id, dst_ip) here
///      via <see cref="RegisterAdoption"/>.
///   3. A background task on the coordinator polls
///      <c>GetAdoptionStatus</c> on each dst; on COMPLETED or FAILED it
///      calls <see cref="MarkAdoptionTerminal"/>.
///   4. The agent's retire endpoint polls
///      <c>GET /admin/retirement-status?pod=&lt;name&gt;</c> until the
///      tracker reports <see cref="GetStatus"/>.Complete = true.
///
/// Thread-safety:
///   The tracker is shared between the rebalance poll loop, the
///   completion poller, and the HTTP endpoint. All mutating ops are
///   protected by per-pod locks; reads use snapshot copies. The
///   underlying outer dictionary is a <see cref="ConcurrentDictionary{TKey,TValue}"/>
///   to keep AddOrUpdate cheap.
///
/// Memory:
///   Entries are kept until <see cref="Forget"/> is called or
///   <see cref="PruneOlderThan"/> ages them out. Forgetting on completion
///   would race with slow agents still polling for their own status; the
///   pruning approach keeps recent retirements visible long enough for
///   the agent to observe completion before exit.
/// </summary>
public static class RetirementTracker
{
    private static readonly ConcurrentDictionary<string, RetirementState> _byPod = new();

    private sealed class RetirementState
    {
        public readonly object Sync = new();
        public DateTime StartedUtc { get; set; } = DateTime.UtcNow;
        public DateTime LastUpdateUtc { get; set; } = DateTime.UtcNow;

        // adoptionId → status snapshot. We keep finished ones around
        // (instead of removing) so /admin/retirement-status can still
        // report the totals after completion.
        public Dictionary<string, AdoptionRecord> Adoptions { get; } = new();
    }

    private sealed class AdoptionRecord
    {
        public string DstName { get; set; } = "";
        public string DstIp { get; set; } = "";
        public string SourceIp { get; set; } = "";
        public DateTime StartedUtc { get; set; } = DateTime.UtcNow;
        public DateTime LastPolledUtc { get; set; } = DateTime.MinValue;
        public string Status { get; set; } = "RUNNING"; // RUNNING | COMPLETED | FAILED
        public string? FailReason { get; set; }
    }

    /// <summary>Record that we've dispatched an adoption against <paramref name="leavingPod"/> on <paramref name="dstName"/>.</summary>
    public static void RegisterAdoption(string leavingPod, string adoptionId, string dstName, string dstIp, string sourceIp)
    {
        if (string.IsNullOrEmpty(leavingPod) || string.IsNullOrEmpty(adoptionId)) return;
        var state = _byPod.GetOrAdd(leavingPod, _ => new RetirementState());
        lock (state.Sync)
        {
            state.Adoptions[adoptionId] = new AdoptionRecord
            {
                DstName = dstName,
                DstIp = dstIp,
                SourceIp = sourceIp,
                StartedUtc = DateTime.UtcNow,
                Status = "RUNNING"
            };
            state.LastUpdateUtc = DateTime.UtcNow;
        }
    }

    /// <summary>Move an adoption to a terminal state (COMPLETED or FAILED). Idempotent.</summary>
    public static void MarkAdoptionTerminal(string leavingPod, string adoptionId, string status, string? failReason = null)
    {
        if (!_byPod.TryGetValue(leavingPod, out var state)) return;
        lock (state.Sync)
        {
            if (!state.Adoptions.TryGetValue(adoptionId, out var rec)) return;
            rec.Status = status;
            rec.FailReason = failReason;
            rec.LastPolledUtc = DateTime.UtcNow;
            state.LastUpdateUtc = DateTime.UtcNow;
        }
    }

    /// <summary>Update polling timestamp without changing status. Useful for liveness logging.</summary>
    public static void TouchAdoption(string leavingPod, string adoptionId)
    {
        if (!_byPod.TryGetValue(leavingPod, out var state)) return;
        lock (state.Sync)
        {
            if (state.Adoptions.TryGetValue(adoptionId, out var rec))
            {
                rec.LastPolledUtc = DateTime.UtcNow;
            }
        }
    }

    /// <summary>Snapshot for the /admin/retirement-status endpoint.</summary>
    public static RetirementStatusSnapshot GetStatus(string leavingPod)
    {
        if (string.IsNullOrEmpty(leavingPod) || !_byPod.TryGetValue(leavingPod, out var state))
        {
            // No tracked retirement → either we never saw the drain (gateway
            // hasn't observed the etcd transition yet) or it's already been
            // pruned. Returning Complete=false here would deadlock the agent
            // poller in the "etcd hasn't propagated yet" case; we return
            // Complete=true with Reason="not tracked" so the agent's poll
            // loop will surface the situation in a log line and proceed.
            //
            // This is the safer default: if the gateway has nothing to do
            // for this pod (e.g. the agent flipped to draining but ring was
            // already empty / single-pod cluster), we should not block
            // termination forever.
            return new RetirementStatusSnapshot
            {
                Pod = leavingPod,
                Total = 0,
                Pending = 0,
                Completed = 0,
                Failed = 0,
                Complete = true,
                Reason = "not tracked"
            };
        }

        lock (state.Sync)
        {
            int total = state.Adoptions.Count;
            int pending = 0, completed = 0, failed = 0;
            foreach (var rec in state.Adoptions.Values)
            {
                switch (rec.Status)
                {
                    case "COMPLETED": completed++; break;
                    case "FAILED": failed++; break;
                    default: pending++; break;
                }
            }
            return new RetirementStatusSnapshot
            {
                Pod = leavingPod,
                Total = total,
                Pending = pending,
                Completed = completed,
                Failed = failed,
                Complete = pending == 0 && total > 0,
                StartedUtc = state.StartedUtc,
                LastUpdateUtc = state.LastUpdateUtc,
                Reason = total == 0 ? "no adoptions dispatched" : null
            };
        }
    }

    /// <summary>Iterate all tracked pods (for the polling loop). Returns names only.</summary>
    public static IReadOnlyList<string> ListTrackedPods()
    {
        return _byPod.Keys.ToList();
    }

    /// <summary>Iterate adoptions for a pod (for the polling loop). Returns shallow snapshots.</summary>
    public static IReadOnlyList<(string AdoptionId, string DstIp, string Status)> ListAdoptions(string pod)
    {
        if (!_byPod.TryGetValue(pod, out var state)) return Array.Empty<(string, string, string)>();
        lock (state.Sync)
        {
            return state.Adoptions
                .Select(kv => (kv.Key, kv.Value.DstIp, kv.Value.Status))
                .ToList();
        }
    }

    /// <summary>Drop tracking for <paramref name="pod"/>. Used by tests; production prefers <see cref="PruneOlderThan"/>.</summary>
    public static void Forget(string pod)
    {
        _byPod.TryRemove(pod, out _);
    }

    /// <summary>Drop pods last-updated more than <paramref name="age"/> ago. Cheap; safe to call from a heartbeat.</summary>
    public static int PruneOlderThan(TimeSpan age)
    {
        var cutoff = DateTime.UtcNow - age;
        int n = 0;
        foreach (var key in _byPod.Keys)
        {
            if (_byPod.TryGetValue(key, out var state))
            {
                lock (state.Sync)
                {
                    if (state.LastUpdateUtc < cutoff)
                    {
                        _byPod.TryRemove(key, out _);
                        n++;
                    }
                }
            }
        }
        return n;
    }
}

public sealed class RetirementStatusSnapshot
{
    public string Pod { get; init; } = "";
    public int Total { get; init; }
    public int Pending { get; init; }
    public int Completed { get; init; }
    public int Failed { get; init; }
    public bool Complete { get; init; }
    public DateTime? StartedUtc { get; init; }
    public DateTime? LastUpdateUtc { get; init; }
    public string? Reason { get; init; }
}
