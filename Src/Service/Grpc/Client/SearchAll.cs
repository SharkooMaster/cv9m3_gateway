
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
    private static readonly object _agentResolveLock = new object();
    private static DateTime _agentResolveAt = DateTime.MinValue;
    private static string[] _resolvedAgents = Array.Empty<string>();
    private static int _roundRobinCounter = -1;

    // Local-mode forced spread for benchmarking:
    // strict round-robin over resolved agent pod IPs.
    private static string SelectAgentForBucket(string _bucketKey)
    {
        if (!LocalModeDetector.IsLocalMode())
        {
            return Globals.AgentsLoadbalancer;
        }

        var now = DateTime.UtcNow;
        lock (_agentResolveLock)
        {
            if (_resolvedAgents.Length == 0 || now - _agentResolveAt > TimeSpan.FromSeconds(15))
            {
                try
                {
                    _resolvedAgents = Dns.GetHostAddresses(Globals.AgentsLoadbalancer)
                        .Where(ip => ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                        .Select(ip => ip.ToString())
                        .Distinct()
                        .ToArray();
                    _agentResolveAt = now;
                    Console.WriteLine($"[SearchAll] Resolved {Globals.AgentsLoadbalancer} => [{string.Join(", ", _resolvedAgents)}]");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[SearchAll] DNS resolve failed for {Globals.AgentsLoadbalancer}: {ex.Message}");
                    // Keep the previous resolved list if we already have one;
                    // transient DNS failures should not collapse routing back to a single service target.
                    _agentResolveAt = now;
                }
            }
        }

        if (_resolvedAgents.Length == 0)
        {
            return Globals.AgentsLoadbalancer;
        }

        int idx = Math.Abs(Interlocked.Increment(ref _roundRobinCounter)) % _resolvedAgents.Length;
        return _resolvedAgents[idx];
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

        if (incoming.Results.Count > 1)
        {
            // Multiple results — byte-level comparison to pick the true best
            int bestByteSim = 0;
            for (int i = 0; i < incoming.Results.Count; i++)
            {
                var r = incoming.Results[i];
                if (r.Chunk == null || r.Chunk.Length == 0) continue;
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
        else if (best.Chunk != null && best.Chunk.Length > 0)
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

        return ret;
    }

    public override async Task<QueryResponse> SearchAll(QueryRequest request, ServerCallContext context)
    {
        QueryResponse response = new QueryResponse();
        ConcurrentBag<QueryResponseObject> responseObjects = new ConcurrentBag<QueryResponseObject>();

        // Create a class that can hold a query and its neighbours  [x]
        List<Query> queries = new List<Query>();
        for (int i = 0; i < request.QueryObjects.Count(); i++)
        {
            var qObj = request.QueryObjects[i];
            queries.Add(new Query()
            {
                query = qObj,
                buckets = GetNeighbouringBuckets(qObj.BucketString, qObj.Vector)
            });
        }
        // Search   []
        ConcurrentBag<(SearchVectorObject, int)> saveQueue = new ConcurrentBag<(SearchVectorObject, int)>();

        // No cap: Use unlimited parallelism for I/O-bound parallel queries
        ParallelOptions options = new ParallelOptions() { MaxDegreeOfParallelism = -1 }; // -1 = unlimited
        await Parallel.ForAsync(0, queries.Count, options, async (i, ct) =>
        {
            // LOCAL MODE: Skip DHT routing, go directly to agent-1
            // DISTRIBUTED MODE: Use DHT to find responsible agent
            string targetAgent;
            if (LocalModeDetector.IsLocalMode())
            {
                targetAgent = SelectAgentForBucket(queries[i].query.BucketString);
            }
            else
            {
                // DHT-AWARE ROUTING: Find the agent responsible for the primary bucket
                targetAgent = await Globals.dhtService.FindResponsibleAgentAsync(
                    queries[i].query.BucketString, 
                    Globals.AgentsLoadbalancer, 
                    ct
                );
            }
            
            SearchVector_Req req = new SearchVector_Req()
            {
                Index = queries[i].query.Index,
                MinimumSimilarity = Globals.MinThresh,
                K = Globals.K
            };
            req.Vector.AddRange(queries[i].query.Vector);
            req.Bitstrings.AddRange(queries[i].buckets);
            SearchVector_Result res = await Globals.svs.ClientGet(req, targetAgent);

            if (res.Save)
            {
                SearchVectorObject svr = res.Results[0];
                // Insert actual chunk into responseObject
                svr.Chunk = request.QueryObjects[svr.Index].Chunk;
                // Add to save queue
                saveQueue.Add((svr, i));
                // Insert to response
                responseObjects.Add(new QueryResponseObject()
                {
                    BucketId = svr.BucketId,
                    BucketKey = (ulong)svr.BucketKey,
                    Similarity = svr.Similarity,
                    Chunk = svr.Chunk,
                    Index = svr.Index,
                    Duplicate = true
                });
            }
            else
            {
                // Filter for best results per query class per response
                QueryResponseObject bestFit = SelectBestResult(res, queries[i]);
                responseObjects.Add(bestFit);
            }
        });

        // Save the results that needs saving - call Agent to store chunks
        foreach ((SearchVectorObject,int) item in saveQueue)
        {
            try
            {
                // LOCAL MODE: Skip DHT routing, go directly to agent-1
                // DISTRIBUTED MODE: Use DHT to find responsible agent
                string targetAgent;
                if (LocalModeDetector.IsLocalMode())
                {
                    targetAgent = SelectAgentForBucket(queries[item.Item2].query.BucketString);
                }
                else
                {
                    // Find the DHT-determined agent for this bucket
                    targetAgent = await Globals.dhtService.FindResponsibleAgentAsync(
                        queries[item.Item2].query.BucketString, 
                        Globals.AgentsLoadbalancer
                    );
                }
                
                StoreVector_Req storeReq = new StoreVector_Req
                {
                    TargetIp = targetAgent, // Use DHT-determined agent for storage
                    Bitstring = queries[item.Item2].query.BucketString,
                    HeadRouteID = ""
                };
                storeReq.Vector.AddRange(queries[item.Item2].query.Vector);
                storeReq.Chunk = item.Item1.Chunk;
                
                var storeRes = Globals.svec.Store(storeReq);
            }
            catch (Exception)
            {
                // Continue anyway - chunk metadata might already be stored
            }
        }

        // Respond properly
        response.Results.AddRange(responseObjects);
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
            // OPTIMIZATION: Process queries in parallel instead of sequentially
            // This is the critical fix - allows multiple queries to be processed concurrently
            
            // Create channel to buffer queries and results
            var queryChannel = Channel.CreateUnbounded<QueryObject>();
            var resultChannel = Channel.CreateUnbounded<(QueryResponseObject? result, int index)>();
            
            // Background task to read queries from stream
            var readTask = Task.Run(async () =>
            {
                var ingestSw = Stopwatch.StartNew();
                try
                {
                    await foreach (var queryObj in requestStream.ReadAllAsync(context.CancellationToken))
                    {
                        await queryChannel.Writer.WriteAsync(queryObj, context.CancellationToken);
                    }
                    queryChannel.Writer.Complete();
                }
                catch (Exception ex)
                {
                    queryChannel.Writer.Complete(ex);
                }
                finally
                {
                    ingestSw.Stop();
                    Observability.RecordStage("Ingress", ingestSw.Elapsed.TotalMilliseconds);
                }
            }, context.CancellationToken);
            
            // Process queries in parallel - NO ARTIFICIAL CAPS
            // Let the system use as much concurrency as it can handle
            // DYNAMIC: Adjust concurrency based on current CPU and memory usage only
            int baseConcurrency = Environment.ProcessorCount * 4; // 4x for I/O-bound (network calls)
            var dynamicConcurrency = DynamicResourceManager.GetOptimalConcurrency(
                minConcurrency: Environment.ProcessorCount, // Start with all cores
                maxConcurrency: int.MaxValue, // No artificial cap
                baseConcurrency: baseConcurrency
            );
            var maxConcurrency = Math.Min(dynamicConcurrency, GetStreamMaxConcurrency());
            var semaphore = new SemaphoreSlim(maxConcurrency, maxConcurrency);
            var activeTasks = new List<Task>();
            
            // Start background task to process queries and stream results
            var processTask = Task.Run(async () =>
            {
                try
                {
                    // Process queries as they arrive
                    await foreach (var queryObj in queryChannel.Reader.ReadAllAsync(context.CancellationToken))
                    {
                        // Wait for semaphore slot
                        await semaphore.WaitAsync(context.CancellationToken);
                        
                        // Process query in parallel
                        var task = Task.Run(async () =>
                        {
                            var querySw = Stopwatch.StartNew();
                            try
                            {
                                using var querySpan = Observability.StartStage("ProcessQuery");
                                querySpan?.SetTag("query.index", queryObj.Index);
                                // Get neighboring buckets for this query (with optimal ordering based on LSH vector confidence)
                                var routeSw = Stopwatch.StartNew();
                                List<string> buckets = GetNeighbouringBuckets(queryObj.BucketString, queryObj.Vector);
                                
                                // LOCAL MODE: Skip DHT routing, go directly to agent-1
                                // DISTRIBUTED MODE: Use DHT to find responsible agent
                                string targetAgent;
                                if (LocalModeDetector.IsLocalMode())
                                {
                                    targetAgent = SelectAgentForBucket(queryObj.BucketString);
                                }
                                else
                                {
                                    // DHT-AWARE ROUTING: Find the agent responsible for the primary bucket
                                    // This distributes load across all agents instead of bottlenecking on agent-1
                                    targetAgent = await Globals.dhtService.FindResponsibleAgentAsync(
                                        queryObj.BucketString, 
                                        Globals.AgentsLoadbalancer, 
                                        context.CancellationToken
                                    );
                                }
                                routeSw.Stop();
                                Observability.RecordStage("Route", routeSw.Elapsed.TotalMilliseconds, ("query_index", queryObj.Index));
                                
                                // Create search request
                                SearchVector_Req req = new SearchVector_Req()
                                {
                                    Index = queryObj.Index,
                                    MinimumSimilarity = Globals.MinThresh,
                                    K = Globals.K
                                };
                                req.Vector.AddRange(queryObj.Vector);
                                req.Bitstrings.AddRange(buckets);
                                
                                // Search in the DHT-determined agent (distributes load across all agents)
                                var searchSw = Stopwatch.StartNew();
                                SearchVector_Result res = await Globals.svs.ClientGet(req, targetAgent, "5000", context.CancellationToken);
                                searchSw.Stop();
                                Observability.RecordStage("SearchBuckets", searchSw.Elapsed.TotalMilliseconds, ("query_index", queryObj.Index));
                                
                                QueryResponseObject responseObj;
                                
                                if (res.Save)
                                {
                                    // OPTIMIZATION: Don't store here - Cross will handle it in background
                                    // Just return flags so Cross knows to store it
                                    responseObj = new QueryResponseObject()
                                    {
                                        BucketId = 0, // Will be set by Cross after booking ID
                                        BucketKey = 0,
                                        Similarity = 1.0f,
                                        Chunk = ByteString.Empty, // No chunk bytes needed
                                        Index = queryObj.Index,
                                        Duplicate = true, // Will be stored, so base == original
                                        NeedToStore = true, // Flag: Cross needs to store this
                                        TargetAgent = targetAgent // Which agent to store to
                                    };
                                }
                                else
                                {
                                    // Find best matching result
                                    Query query = new Query()
                                    {
                                        query = queryObj,
                                        buckets = buckets
                                    };
                                    responseObj = SelectBestResult(res, query);
                                    
                                    // Only store if similarity < MinThresh (no good match found across all searched buckets)
                                    if (responseObj.Similarity < Globals.MinThresh)
                                    {
                                        // OPTIMIZATION: Don't store here - Cross will handle it in background
                                        // Just return flags so Cross knows to store it
                                        responseObj.BucketId = 0; // Will be set by Cross after booking ID
                                        responseObj.BucketKey = 0;
                                        responseObj.Chunk = ByteString.Empty; // No chunk bytes needed
                                        responseObj.Similarity = 1.0f;
                                        responseObj.Duplicate = true; // Will be stored, so base == original
                                        responseObj.NeedToStore = true; // Flag: Cross needs to store this
                                        responseObj.TargetAgent = targetAgent; // Which agent to store to
                                    }
                                    else
                                    {
                                        // Similarity >= MinThresh, found near-duplicate - use it for deduplication, DON'T store new chunk
                                    }
                                }

                                // Write result to channel (stream as ready)
                                var serializeSw = Stopwatch.StartNew();
                                await resultChannel.Writer.WriteAsync((responseObj, queryObj.Index), context.CancellationToken);
                                serializeSw.Stop();
                                Observability.RecordStage("Serialize", serializeSw.Elapsed.TotalMilliseconds, ("query_index", queryObj.Index));
                            }
                            catch (Exception ex)
                            {
                                // Error processing query — attempt emergency store
                                // When search fails, try to store the chunk and use the stored reference.
                                // This prevents the Cross encoder from falling back to raw-embed unnecessarily.
                                ulong storedBucketId = 0;
                                ulong storedBucketKey = 0;
                                try
                                {
                                    string targetAgent;
                                    if (LocalModeDetector.IsLocalMode())
                                    {
                                        targetAgent = SelectAgentForBucket(queryObj.BucketString);
                                    }
                                    else
                                    {
                                        targetAgent = await Globals.dhtService.FindResponsibleAgentAsync(
                                            queryObj.BucketString, 
                                            Globals.AgentsLoadbalancer, 
                                            context.CancellationToken
                                        );
                                    }
                                    
                                    StoreVector_Req storeReq = new StoreVector_Req
                                    {
                                        TargetIp = targetAgent,
                                        Bitstring = queryObj.BucketString,
                                        HeadRouteID = ""
                                    };
                                    storeReq.Vector.AddRange(queryObj.Vector);
                                    storeReq.Chunk = queryObj.Chunk;
                                    
                                    var callOptions = new CallOptions(
                                        deadline: DateTime.UtcNow.AddSeconds(10),
                                        cancellationToken: context.CancellationToken
                                    );
                                    var storeRes = Globals.svec.Store(storeReq, callOptions);
                                    storedBucketId = storeRes.Id;
                                    storedBucketKey = storeRes.Index;
                                    // Emergency store succeeded
                                }
                                catch (Exception storeEx)
                                {
                                    // Emergency store also failed
                                }
                                
                                // Return response with stored reference (if store succeeded) or 0,0 (raw-embed fallback).
                                // Either way, Chunk = original data so Cross encoder can use it.
                                var errorResponseObj = new QueryResponseObject()
                                {
                                    BucketId = storedBucketId,
                                    BucketKey = storedBucketKey,
                                    Similarity = storedBucketId > 0 ? 1.0f : 0f,
                                    Chunk = queryObj.Chunk,
                                    Index = queryObj.Index,
                                    Duplicate = storedBucketId > 0 // base == original when store succeeded
                                };
                                await resultChannel.Writer.WriteAsync((errorResponseObj, queryObj.Index), context.CancellationToken);
                            }
                            finally
                            {
                                querySw.Stop();
                                Observability.RecordStage("Egress", querySw.Elapsed.TotalMilliseconds, ("query_index", queryObj.Index));
                                semaphore.Release();
                            }
                        }, context.CancellationToken);
                        
                        activeTasks.Add(task);
                    }
                    
                    // Wait for all processing tasks to complete
                    await Task.WhenAll(activeTasks);
                    
                    // Close result channel writer
                    resultChannel.Writer.Complete();
                }
                catch (Exception ex)
                {
                    resultChannel.Writer.Complete(ex);
                }
            }, context.CancellationToken);
            
            // Stream results as they complete (order doesn't matter - Cross collects by index)
            await foreach (var (result, index) in resultChannel.Reader.ReadAllAsync(context.CancellationToken))
            {
                if (result != null)
                {
                    await responseStream.WriteAsync(result);
                }
            }
            
            // Wait for all tasks to complete
            await Task.WhenAll(readTask, processTask);
        }
        catch (OperationCanceledException)
        {
            // Client cancelled - this is normal, just exit
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SearchAllStream] Fatal error: {ex.Message}");
            throw;
        }
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
