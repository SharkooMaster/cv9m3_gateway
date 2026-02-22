
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text.Json;
using System.Threading.Channels;
using Gateway.Modules;
using Gateway.Modules.Agneta;
using Gateway.Modules.Pushover;
using Gateway.Utils.Globals;
using Gateway.Utils.Misc;
using Gateway.Utils;
using GatewayService;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.AspNetCore.Server.HttpSys;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace Gateway.Services.Grpc;

public class Query
{
  public QueryObject query { get; set; }
  public List<string> buckets { get; set; }
}

public class SearchAllService : GatewayService.GatewayService.GatewayServiceBase
{
    // ── Rendezvous routing: deterministic, zero-network-hop agent selection ──
    // Replaces both round-robin (local mode) and DHT (distributed mode).
    // Same (bucket, agent set) always maps to the same agent.
    // Adding/removing an agent only remaps ~1/N of buckets.
    private static string RouteToAgent(string bucketString)
    {
        return RendezvousRouter.PickAgent(bucketString);
    }

    private static int GetStreamMaxConcurrency()
    {
        var raw = Environment.GetEnvironmentVariable("GATEWAY_STREAM_CONCURRENCY");
        if (int.TryParse(raw, out var configured) && configured > 0)
        {
            return configured;
        }

        // No cap: Use unlimited concurrency for I/O-bound work
        // System will naturally limit based on available resources
        return int.MaxValue;
    }

    private async Task initCLMS(string _name, string _id, string headID)
    {
        _ = ClmsHandler.RegisterRoutePoint(headID, _name, _id);
    }

    private async Task addEvent(string _step, string _message, string headID, string _type = "step", string _level = "1")
    {
        _ = ClmsHandler.AddEventToRoutePoint(headID, new M_CLMSEvent(){
            level = _level, stepName = _step, type = _type, message = _message
        });
    }

    // OPTIMIZATION: Overload without vector (backward compatible)
    private List<string> GetNeighbouringBuckets(string bitString)
    {
        return GetNeighbouringBuckets(bitString, null);
    }
    
    // OPTIMIZATION: Confidence-based bucket ordering for faster early termination
    // Bits with low confidence (low |sum|) are most likely to change, so flip those first
    private List<string> GetNeighbouringBuckets(string bitString, Google.Protobuf.Collections.RepeatedField<float>? lshVector = null)
    {
        // K=1 means radius-1: flip every bit position (all single-bit neighbors)
        // Otherwise, flip the first K bits (legacy behavior)
        var results = new List<string>();
        char[] buffer = bitString.ToCharArray();

        // Always include the original bucket first (most likely to have matches)
        results.Add(bitString);

        // OPTIMIZATION: If we have LSH vector, sort bits by confidence for optimal search order
        if (lshVector != null && lshVector.Count == bitString.Length && Globals.K <= 1)
        {
            // Sort bits by confidence (lowest |sum| = most uncertain = flip first)
            // This helps early termination find matches faster
            var bitConfidences = new List<(int bitIndex, float confidence)>();
            for (int i = 0; i < bitString.Length; i++)
            {
                float confidence = Math.Abs(lshVector[i]);
                bitConfidences.Add((i, confidence));
            }
            
            // Sort by confidence (lowest first - these are most likely to change)
            bitConfidences.Sort((a, b) => a.confidence.CompareTo(b.confidence));
            
            // Still search ALL 64 neighbors, but in optimal order
            foreach (var (bitIndex, _) in bitConfidences)
            {
                char original = buffer[bitIndex];
                buffer[bitIndex] = (original == '0') ? '1' : '0';
                results.Add(new string(buffer));
                buffer[bitIndex] = original; // Flip back
            }
        }
        else if (Globals.K <= 1)
        {
            // Fallback: original sequential order if no vector provided
            for (int i = 0; i < buffer.Length; i++)
            {
                char original = buffer[i];
                buffer[i] = (original == '0') ? '1' : '0';
                results.Add(new string(buffer));
                buffer[i] = original;
            }
        }
        else
        {
            int flips = Math.Min(Globals.K, buffer.Length);
            for (int i = 0; i < flips; i++)
            {
                char original = buffer[i];
                buffer[i] = (original == '0') ? '1' : '0';
                results.Add(new string(buffer));
                buffer[i] = original;
            }
        }

        return results;
    }

    public static unsafe (int[] result, int equalCount) SubtractAVX2(byte[] a, byte[] b)
    {
        if (!Avx2.IsSupported)
            throw new PlatformNotSupportedException("AVX2 is not supported on this CPU.");
        if (a.Length != b.Length)
            throw new ArgumentException("Arrays must be the same length.");

        int length = a.Length;
        int[] result = new int[length];
        int equalCount = 0;

        fixed (byte* pa = a, pb = b)
        fixed (int* pr = result)
        {
            int i = 0;
            int vectorWidth = 32;

            for (; i <= length - vectorWidth; i += vectorWidth)
            {
                // Load 32 bytes from each array
                Vector256<byte> va = Avx.LoadVector256(pa + i);
                Vector256<byte> vb = Avx.LoadVector256(pb + i);

                // Compare for equality (a == b)
                Vector256<byte> cmp = Avx2.CompareEqual(va, vb);

                // Count number of matching bytes (cmp byte = 0xFF when equal)
                uint mask = (uint)Avx2.MoveMask(cmp); // 32 bits
                equalCount += CountSetBits(mask);

                // Manual subtraction: split 128-bit lanes into ushort vectors
                var va_low = Avx.ExtractVector128(va, 0);
                var va_high = Avx.ExtractVector128(va, 1);
                var vb_low = Avx.ExtractVector128(vb, 0);
                var vb_high = Avx.ExtractVector128(vb, 1);

                for (int j = 0; j < 16; j++)
                {
                    pr[i + j] = va_low.GetElement(j) - vb_low.GetElement(j);
                    pr[i + 16 + j] = va_high.GetElement(j) - vb_high.GetElement(j);
                }
            }

            // Remainder
            for (; i < length; i++)
            {
                int diff = pa[i] - pb[i];
                pr[i] = diff;
                if (diff == 0)
                    equalCount++;
            }
        }

        return (result, equalCount);
    }

    // Efficient Hamming weight (population count)
    private static int CountSetBits(uint value)
    {
        // .NET 5+ has BitOperations.PopCount, but we use a manual fallback
        int count = 0;
        while (value != 0)
        {
            count += (int)(value & 1);
            value >>= 1;
        }
        return count;
    }

    private QueryResponseObject SelectBestResult(SearchVector_Result incoming, Query query)
    {
        QueryResponseObject ret = new QueryResponseObject();

        if (incoming.Results == null || incoming.Results.Count == 0)
        {
            ret.BucketId = 0;
            ret.BucketKey = 0;
            ret.Chunk = query.query.Chunk;
            ret.Index = query.query.Index;
            ret.Similarity = 0.0f;
            ret.Duplicate = false;
            return ret;
        }

        // With K=1 the agent already picked the best cosine match.
        // Only run SubtractAVX2 when we have multiple results AND chunk bytes.
        var best = incoming.Results[0];
        float bestSim = best.Similarity;
        int bestIndex = 0;
        bool hasQueryChunk = query.query.Chunk != null && query.query.Chunk.Length > 0;

        if (incoming.Results.Count > 1)
        {
            if (hasQueryChunk)
            {
                // Multiple results — byte-level comparison to pick the true best
                int bestByteSim = 0;
                for (int i = 0; i < incoming.Results.Count; i++)
                {
                    var r = incoming.Results[i];
                    if (r.Chunk == null || r.Chunk.Length == 0) continue;
                    if (r.Chunk.Length != query.query.Chunk.Length) continue;
                    (int[] _, int eqCount) = SubtractAVX2(query.query.Chunk.ToArray(), r.Chunk.ToArray());
                    if (eqCount > bestByteSim)
                    {
                        bestByteSim = eqCount;
                        bestIndex = i;
                    }
                }
                best = incoming.Results[bestIndex];
                if (bestByteSim > 0 && best.Chunk.Length > 0)
                    bestSim = (float)bestByteSim / best.Chunk.Length;
            }
            else
            {
                // Two-phase mode: no query chunk bytes available.
                // Use cosine similarity only and pick max.
                for (int i = 1; i < incoming.Results.Count; i++)
                {
                    if (incoming.Results[i].Similarity > bestSim)
                    {
                        best = incoming.Results[i];
                        bestSim = best.Similarity;
                    }
                }
            }
        }
        else if (hasQueryChunk && best.Chunk != null && best.Chunk.Length > 0 && best.Chunk.Length == query.query.Chunk.Length)
        {
            // Single result with chunk bytes — compute exact byte-level similarity
            (int[] _, int eqCount) = SubtractAVX2(query.query.Chunk.ToArray(), best.Chunk.ToArray());
            bestSim = (float)eqCount / best.Chunk.Length;
        }
        // else: no chunk bytes — use cosine similarity from agent (already in best.Similarity)

        ret.BucketId = best.BucketId;
        ret.BucketKey = (ulong)best.BucketKey;
        ret.Chunk = (best.Chunk != null && best.Chunk.Length > 0) ? best.Chunk : query.query.Chunk;
        ret.Index = best.Index;
        ret.Similarity = bestSim;
        ret.Duplicate = bestSim >= Globals.MinThresh;
        ret.StorageGuid = best.StorageGuid ?? "";

        return ret;
    }

    public override async Task<QueryResponse> SearchAll(QueryRequest request, ServerCallContext context)
    {
        var response = new QueryResponse();
        if (request.QueryObjects.Count == 0) return response;

        // Route & group queries by agent
        var queryInfos = new (QueryObject query, List<string> buckets, string agent)[request.QueryObjects.Count];
        Parallel.For(0, request.QueryObjects.Count, i =>
        {
            var q = request.QueryObjects[i];
            var buckets = GetNeighbouringBuckets(q.BucketString, q.Vector);
            var agent = RouteToAgent(q.BucketString);
            queryInfos[i] = (q, buckets, agent);
        });

        var agentGroups = new Dictionary<string, List<int>>();
        for (int i = 0; i < queryInfos.Length; i++)
        {
            if (!agentGroups.TryGetValue(queryInfos[i].agent, out var list))
            {
                list = new List<int>();
                agentGroups[queryInfos[i].agent] = list;
            }
            list.Add(i);
        }

        // Batch search per agent
        var results = new QueryResponseObject[request.QueryObjects.Count];
        var tasks = new List<Task>();
        foreach (var (agent, indices) in agentGroups)
        {
            var a = agent; var idxs = indices;
            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    var batchReq = new BatchSearchVector_Req();
                    foreach (var idx in idxs)
                    {
                        var info = queryInfos[idx];
                        var req = new SearchVector_Req { Index = info.query.Index, MinimumSimilarity = Globals.MinThresh, K = Globals.K };
                        req.Vector.AddRange(info.query.Vector);
                        req.Bitstrings.AddRange(info.buckets);
                        batchReq.Queries.Add(req);
                    }
                    var batchRes = await Globals.svs.ClientBatchGet(batchReq, a, context.CancellationToken);
                    for (int j = 0; j < idxs.Count && j < batchRes.Results.Count; j++)
                    {
                        results[idxs[j]] = BuildResponseObj(batchRes.Results[j], queryInfos[idxs[j]].query, queryInfos[idxs[j]].buckets, a);
                    }
                }
                catch
                {
                    foreach (var idx in idxs)
                        results[idx] = new QueryResponseObject { BucketId = 0, BucketKey = 0, Similarity = 1.0f, Chunk = ByteString.Empty, Index = queryInfos[idx].query.Index, Duplicate = true, NeedToStore = true, TargetAgent = a };
                }
            }, context.CancellationToken));
        }
        await Task.WhenAll(tasks);

        // Store any chunks that need storing
        var storeTasks = new List<Task>();
        for (int i = 0; i < results.Length; i++)
        {
            var r = results[i];
            if (r == null) continue;
            if (!r.NeedToStore) { response.Results.Add(r); continue; }

            var idx = i;
            storeTasks.Add(Task.Run(async () =>
            {
                try
                {
                    var info = queryInfos[idx];
                    var storeReq = new StoreVector_Req { TargetIp = r.TargetAgent, Bitstring = info.query.BucketString, HeadRouteID = "" };
                    storeReq.Vector.AddRange(info.query.Vector);
                    if (info.query.Chunk != null && info.query.Chunk.Length > 0)
                        storeReq.Chunk = info.query.Chunk;
                    else return;
                    var storeRes = await Task.FromResult(Globals.svec.Store(storeReq));
                    r.BucketId = storeRes.Id;
                    r.BucketKey = storeRes.Index;
                    r.StorageGuid = storeRes.StorageGuid ?? "";
                }
                catch { /* Continue — chunk may already be stored */ }
            }));
            response.Results.Add(r);
        }
        if (storeTasks.Count > 0)
            await Task.WhenAll(storeTasks);

        return response;
    }

    public override async Task SearchAllStream(
        IAsyncStreamReader<QueryObject> requestStream,
        IServerStreamWriter<QueryResponseObject> responseStream,
        ServerCallContext context)
    {
        using var rootSpan = Observability.StartStage("SearchAllStream");
        try
        {
            // ──────────────────────────────────────────────────────────────────
            // PHASE 1: Ingest ALL queries from the stream into memory.
            // Queries carry NO chunk bytes (two-phase mode), so this is fast.
            // For a 3MB file ≈ 600 queries × ~540 bytes each ≈ 320KB total.
            // ──────────────────────────────────────────────────────────────────
            var ingestSw = Stopwatch.StartNew();
            var allQueries = new List<QueryObject>();
            await foreach (var queryObj in requestStream.ReadAllAsync(context.CancellationToken))
            {
                allQueries.Add(queryObj);
            }
            ingestSw.Stop();
            Observability.RecordStage("Ingress", ingestSw.Elapsed.TotalMilliseconds);

            if (allQueries.Count == 0) return;

            // ──────────────────────────────────────────────────────────────────
            // PHASE 2: Group queries by target agent (rendezvous hash).
            // Each group becomes ONE BatchGet gRPC call.
            // With 5 agents: ~600 queries → 5 gRPC calls (instead of 600).
            // ──────────────────────────────────────────────────────────────────
            var routeSw = Stopwatch.StartNew();

            // For each query: compute bitstring neighbors, route to agent
            var queryInfos = new (QueryObject query, List<string> buckets, string agent)[allQueries.Count];

            // Parallel routing — pure CPU, no I/O
            Parallel.For(0, allQueries.Count, i =>
            {
                var q = allQueries[i];
                var buckets = GetNeighbouringBuckets(q.BucketString, q.Vector);
                var agent = RouteToAgent(q.BucketString);
                queryInfos[i] = (q, buckets, agent);
            });

            // Group by agent
            var agentGroups = new Dictionary<string, List<int>>(); // agent -> list of query indices
            for (int i = 0; i < queryInfos.Length; i++)
            {
                var agent = queryInfos[i].agent;
                if (!agentGroups.TryGetValue(agent, out var list))
                {
                    list = new List<int>();
                    agentGroups[agent] = list;
                }
                list.Add(i);
            }

            routeSw.Stop();
            Observability.RecordStage("Route", routeSw.Elapsed.TotalMilliseconds);

            // ──────────────────────────────────────────────────────────────────
            // PHASE 3: Fire ONE BatchGet per agent — in parallel.
            // All results come back in one shot per agent.
            // ──────────────────────────────────────────────────────────────────
            var searchSw = Stopwatch.StartNew();

            // Array to hold all results, indexed by original query position
            var results = new QueryResponseObject[allQueries.Count];

            var batchTasks = new List<Task>();
            foreach (var (agent, queryIndices) in agentGroups)
            {
                var capturedAgent = agent;
                var capturedIndices = queryIndices;

                batchTasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        // Build the batch request
                        var batchReq = new BatchSearchVector_Req();
                        foreach (var idx in capturedIndices)
                        {
                            var info = queryInfos[idx];
                            var req = new SearchVector_Req
                            {
                                Index = info.query.Index,
                                MinimumSimilarity = Globals.MinThresh,
                                K = Globals.K
                            };
                            req.Vector.AddRange(info.query.Vector);
                            req.Bitstrings.AddRange(info.buckets);
                            batchReq.Queries.Add(req);
                        }

                        // ONE gRPC call for all queries to this agent
                        var batchRes = await Globals.svs.ClientBatchGet(batchReq, capturedAgent, context.CancellationToken);

                        // Map results back to original query positions
                        for (int j = 0; j < capturedIndices.Count && j < batchRes.Results.Count; j++)
                        {
                            var idx = capturedIndices[j];
                            var res = batchRes.Results[j];
                            var info = queryInfos[idx];

                            results[idx] = BuildResponseObj(res, info.query, info.buckets, capturedAgent);
                        }
                    }
                    catch (Exception)
                    {
                        // On batch failure: mark all queries in this group as need-to-store
                        foreach (var idx in capturedIndices)
                        {
                            results[idx] = new QueryResponseObject
                            {
                                BucketId = 0,
                                BucketKey = 0,
                                Similarity = 1.0f,
                                Chunk = ByteString.Empty,
                                Index = queryInfos[idx].query.Index,
                                Duplicate = true,
                                NeedToStore = true,
                                TargetAgent = capturedAgent
                            };
                        }
                    }
                }, context.CancellationToken));
            }

            await Task.WhenAll(batchTasks);
            searchSw.Stop();
            Observability.RecordStage("SearchBuckets", searchSw.Elapsed.TotalMilliseconds);

            // ──────────────────────────────────────────────────────────────────
            // PHASE 4: Stream all results back to Cross.
            // Order doesn't matter — Cross collects by index.
            // ──────────────────────────────────────────────────────────────────
            var egressSw = Stopwatch.StartNew();
            for (int i = 0; i < results.Length; i++)
            {
                var r = results[i];
                if (r != null)
                {
                    await responseStream.WriteAsync(r);
                }
                else
                {
                    // Safety: if somehow a result slot was missed
                    await responseStream.WriteAsync(new QueryResponseObject
                    {
                        BucketId = 0,
                        BucketKey = 0,
                        Similarity = 1.0f,
                        Chunk = ByteString.Empty,
                        Index = allQueries[i].Index,
                        Duplicate = true,
                        NeedToStore = true,
                        TargetAgent = queryInfos[i].agent
                    });
                }
            }
            egressSw.Stop();
            Observability.RecordStage("Egress", egressSw.Elapsed.TotalMilliseconds);
        }
        catch (OperationCanceledException)
        {
            // Client cancelled - normal
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SearchAllStream] Fatal error: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// Convert an agent's SearchVector_Result into a QueryResponseObject.
    /// Handles both "save needed" and "match found" cases.
    /// </summary>
    private QueryResponseObject BuildResponseObj(SearchVector_Result res, QueryObject queryObj, List<string> buckets, string targetAgent)
    {
        if (res.Save)
        {
            return new QueryResponseObject
            {
                BucketId = 0,
                BucketKey = 0,
                Similarity = 1.0f,
                Chunk = ByteString.Empty,
                Index = queryObj.Index,
                Duplicate = true,
                NeedToStore = true,
                TargetAgent = targetAgent
            };
        }

        var query = new Query { query = queryObj, buckets = buckets };
        var responseObj = SelectBestResult(res, query);

        // ALWAYS set TargetAgent so Cross knows which agent to fetch base chunks from
        // during diff encoding (lazy fetch) and decompression.
        responseObj.TargetAgent = targetAgent;

        if (responseObj.Similarity < Globals.MinThresh)
        {
            // Below threshold — needs storing
            responseObj.BucketId = 0;
            responseObj.BucketKey = 0;
            responseObj.Chunk = ByteString.Empty;
            responseObj.Similarity = 1.0f;
            responseObj.Duplicate = true;
            responseObj.NeedToStore = true;
        }

        return responseObj;
    }
    
/*     public override async Task<QueryResponse> SearchAll(QueryRequest request, ServerCallContext context)
    {
        try
        {
            string headID = request.HeadRouteID;
            _ = initCLMS("Gateway", "A2", headID);

            QueryResponse response = new QueryResponse();
            ConcurrentBag<QueryResponseObject> resultsBag = new ConcurrentBag<QueryResponseObject>();

            // get neighbour buckets
            QueryObject req = request.QueryObjects[0];
            List<string> neighbouringBuckets = GetNeighbouringBuckets(req.BucketString, req.Vector);
            ConcurrentBag<SearchVectorObject> searchResults = new ConcurrentBag<SearchVectorObject>();
            string targetIP = Globals.AgentsLoadbalancer;

            ParallelOptions options = new ParallelOptions() { MaxDegreeOfParallelism = Math.Min(neighbouringBuckets.Count, Environment.ProcessorCount * 2) };
            await Parallel.ForAsync(0, neighbouringBuckets.Count, options, async (i, ct) => {
                _ = addEvent("SearchAll:Start", $"Preparing search_vector_request {i}", headID);

                SearchVector_Req searchReq = new SearchVector_Req
                {
                    Bitstring = neighbouringBuckets[i],
                    K = Globals.K,
                    MinimumSimilarity = Globals.MinThresh,
                    HeadRouteID = headID
                };
                searchReq.Vector.AddRange(req.Vector);

                // Searching
                try
                {
                    SearchVector_Result? _res = null;
                    string route_ip = Globals.AgentsLoadbalancer;
                    bool reroute = true;
                    while (reroute)
                    {
                        _ = addEvent("SearchAll:Searching", $"Routing search to {route_ip} | {neighbouringBuckets[i]}", headID);
                        _res = await Globals.svs.ClientGet(
                            searchReq,
                            route_ip,
                            (route_ip == Globals.AgentsLoadbalancer) ? "80" : "5000",
                            context.CancellationToken
                        );

                        route_ip = _res.TargetIp;
                        reroute = _res.Forward;
                        if(searchReq.Bitstring == req.BucketString){ targetIP = route_ip; }
                    }
                    if(_res != null)
                    {
                        foreach (var res in _res.Results)
                        {
                            searchResults.Add(res);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _ = addEvent("SearchAll:FailedToSearch", $"Request to search for vector failed: {ex.Message} : {ex.Data}", headID);
                    _ = ClmsHandler.SendRoutePoint(headID);
                    throw;
                }
            });

            if(searchResults.Count == 0)
            {
                _ = addEvent("Searched:Nothing Found", "No result found. Saving", headID);
                StoreVector_Req svecReq = new StoreVector_Req
                {
                    TargetIp = targetIP,
                    Bitstring = req.BucketString,
                    HeadRouteID = headID
                };
                svecReq.Vector.AddRange(req.Vector);

                svecReq.Chunk = req.Chunk;

                _ = addEvent("Searched:Storing", $"Sending results to save", headID);
                ulong vectorIndex = Globals.svec.Store(svecReq, cancellationToken: context.CancellationToken).Id;
                _ = addEvent("Searched:Storing", $"Saved results", headID);

                resultsBag.Add(new QueryResponseObject
                {
                    Id = req.Id,
                    Index = req.Index,
                    Similarity = 1,
                    Chunk = req.Chunk
                });
            }
            else
            {
                _ = addEvent("Searched:Found", "Result found", headID);
                foreach (var result in searchResults)
                {
                    if (result.SimilarityRate >= Globals.MinThresh)
                    {
                        resultsBag.Add(new QueryResponseObject
                        {
                            Id = result.Id,
                            Index = request.QueryObjects[0].Index,
                            Similarity = result.SimilarityRate,
                            Chunk = result.Chunk
                        });
                    }
                }
            }

            _ = addEvent("SearchAll:Final", "Done", headID);
            _ = ClmsHandler.SendRoutePoint(headID);

            response.Results.AddRange(resultsBag);
            return response;
        }
        catch(Exception exc)
        {
            Console.WriteLine(exc);
            PushoverHandler.PushNotification($"Error, gateway process failed: {exc.Data} | {exc.Message}");
            throw;
        }
    } */

}
