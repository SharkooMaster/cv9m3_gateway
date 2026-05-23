using System.Threading;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;

namespace Gateway.Utils;

/// <summary>
/// Exposes <c>GET /stats/runtime</c> on this pod's HTTP/1 port (5001).
///
/// Pulled by the control-center pod every ~30 s. The handler is allocation-bounded
/// and never touches request routing. The shape mirrors what the control-center
/// scraper deserialises into (see <c>RuntimeStatsStore.RuntimeSample</c>) and
/// must stay in sync with the cross / agent endpoints — adding new fields is
/// fine, renaming or repurposing is not.
/// </summary>
public static class RuntimeStatsEndpoint
{
    private static readonly System.Diagnostics.Stopwatch _uptime = System.Diagnostics.Stopwatch.StartNew();

    public static void Map(WebApplication app, string component)
    {
        app.MapGet("/stats/runtime", () =>
        {
            var gc = System.GC.GetGCMemoryInfo();
            var p = System.Diagnostics.Process.GetCurrentProcess();
            p.Refresh();
            var gens = gc.GenerationInfo;

            ThreadPool.GetAvailableThreads(out var workerAvail, out var ioAvail);
            ThreadPool.GetMaxThreads(out var workerMax, out var ioMax);
            ThreadPool.GetMinThreads(out var workerMin, out var ioMin);
            var threadpoolQueue = ThreadPool.PendingWorkItemCount;

            double uptimeSec = _uptime.Elapsed.TotalSeconds;
            double gcPauseSec = 0;
            try { gcPauseSec = System.GC.GetTotalPauseDuration().TotalSeconds; } catch { /* runtime < 7 */ }
            double timeInGcPct = uptimeSec > 0 ? gcPauseSec * 100.0 / uptimeSec : 0;

            int openFdCount = TryReadOpenFdCount();

            return Results.Json(new
            {
                ts_unix_ms              = System.DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                component               = component,
                pod                     = System.Environment.GetEnvironmentVariable("MY_POD_NAME") ?? "unknown",
                node                    = System.Environment.GetEnvironmentVariable("MY_NODE_NAME") ?? "unknown",
                process_uptime_sec      = uptimeSec,

                heap_size_bytes         = gc.HeapSizeBytes,
                heap_committed_bytes    = gc.TotalCommittedBytes,
                heap_fragmented_bytes   = gc.FragmentedBytes,

                gen0_size               = gens.Length > 0 ? gens[0].SizeAfterBytes : 0,
                gen1_size               = gens.Length > 1 ? gens[1].SizeAfterBytes : 0,
                gen2_size               = gens.Length > 2 ? gens[2].SizeAfterBytes : 0,
                loh_size                = gens.Length > 3 ? gens[3].SizeAfterBytes : 0,
                loh_fragmented_bytes    = gens.Length > 3 ? gens[3].FragmentationAfterBytes : 0,
                poh_size                = gens.Length > 4 ? gens[4].SizeAfterBytes : 0,

                gen0_collections        = System.GC.CollectionCount(0),
                gen1_collections        = System.GC.CollectionCount(1),
                gen2_collections        = System.GC.CollectionCount(2),

                gc_pause_total_sec      = gcPauseSec,
                time_in_gc_pct          = timeInGcPct,

                rss_bytes               = p.WorkingSet64,
                private_bytes           = p.PrivateMemorySize64,
                memory_load_bytes       = gc.MemoryLoadBytes,
                memory_load_threshold   = gc.HighMemoryLoadThresholdBytes,
                native_overhead_bytes   = System.Math.Max(0, p.WorkingSet64 - gc.HeapSizeBytes),

                threadpool_workers_busy       = System.Math.Max(0, workerMax - workerAvail),
                threadpool_workers_max        = workerMax,
                threadpool_workers_min        = workerMin,
                threadpool_completion_busy    = System.Math.Max(0, ioMax - ioAvail),
                threadpool_completion_max     = ioMax,
                threadpool_completion_min     = ioMin,
                threadpool_queue_length       = threadpoolQueue,

                open_fd_count           = openFdCount,

                grpc_channel_cache_count = GrpcChannelFactory.ChannelCacheCount,
                grpc_client_cache_count  = GrpcChannelFactory.ClientCacheCount,

                // ── Replication health (Phase 7) ──
                // Reads RingState directly so the values are guaranteed
                // to match what every PickReplicas call sees right now.
                ring_size           = Gateway.Routing.RingState.Current.Agents.Count,
                topology_version    = Gateway.Routing.RingState.TopologyVersion,
                vnodes_per_agent    = Gateway.Routing.RingState.Current.VnodesPerAgent,
                replication_factor  = ParseEnvInt("REPLICATION_FACTOR", 3),
                write_quorum        = ParseEnvInt("WRITE_QUORUM", 2),
                rebalance_coordinator_enabled =
                    string.Equals(System.Environment.GetEnvironmentVariable("REBALANCE_COORDINATOR_ENABLED"),
                        "true", System.StringComparison.OrdinalIgnoreCase),
            });
        });
    }

    private static int ParseEnvInt(string name, int fallback)
    {
        var raw = System.Environment.GetEnvironmentVariable(name);
        return int.TryParse(raw, out var v) && v > 0 ? v : fallback;
    }

    private static int TryReadOpenFdCount()
    {
        try
        {
            const string path = "/proc/self/fd";
            if (!System.IO.Directory.Exists(path)) return 0;
            int count = 0;
            foreach (var _ in System.IO.Directory.EnumerateFileSystemEntries(path)) count++;
            return count;
        }
        catch { return 0; }
    }
}
