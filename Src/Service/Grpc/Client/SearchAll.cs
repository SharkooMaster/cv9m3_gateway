
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text.Json;
using Gateway.Modules;
using Gateway.Modules.Agneta;
using Gateway.Modules.Pushover;
using Gateway.Utils.Globals;
using Gateway.Utils.Misc;
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

    private List<string> GetNeighbouringBuckets(string bitString)
    {
        List<string> results = new(Globals.K + 1);
        char[] buffer = bitString.ToCharArray();

        results.Add(bitString);
        for (int i = 0; i < Globals.K; i++)
        {
            char original = buffer[i];
            buffer[i] = (original == '0') ? '1' : '0';
            results.Add(new string(buffer));
            buffer[i] = original;
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

        int bestIndex = -1;
        int bestIndexSim = 0;
        int[]? bestIndexDeltas = null;
        for (int i = 0; i < incoming.Results.Count; i++)
        {
            (int[], int) subRes = SubtractAVX2(query.query.Chunk.ToArray(), incoming.Results[i].Chunk.ToArray());
            if (subRes.Item2 > bestIndexSim)
            {
                bestIndex = i;
                bestIndexSim = subRes.Item2;
                bestIndexDeltas = subRes.Item1;
            }
        }

        if(bestIndex < 0){ throw new Exception("Error: bestIndex in SelectBestResult resulted in -1. Index our of range"); }

        ret.BucketId = incoming.Results[bestIndex].BucketId;
        ret.BucketKey = (ulong)incoming.Results[bestIndex].BucketKey;
        ret.Chunk = incoming.Results[bestIndex].Chunk;
        ret.Index = incoming.Results[bestIndex].Index;
        ret.Similarity = bestIndexSim / incoming.Results[bestIndex].Chunk.Length;
        ret.Duplicate = false;

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
                buckets = GetNeighbouringBuckets(qObj.BucketString)
            });
        }
        Console.WriteLine($"len: {queries.Count}");

        // Search   []
        ConcurrentBag<(SearchVectorObject, int)> saveQueue = new ConcurrentBag<(SearchVectorObject, int)>();

        ParallelOptions options = new ParallelOptions() { MaxDegreeOfParallelism = 10 };
        await Parallel.ForAsync(0, queries.Count, options, async (i, ct) =>
        {
            SearchVector_Req req = new SearchVector_Req()
            {
                Index = queries[i].query.Index,
                MinimumSimilarity = Globals.MinThresh,
                K = Globals.K
            };
            req.Vector.AddRange(queries[i].query.Vector);
            req.Bitstrings.AddRange(queries[i].buckets);
            SearchVector_Result res = await Globals.svs.ClientGet(req, Globals.AgentsLoadbalancer);

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
                StoreVector_Req storeReq = new StoreVector_Req
                {
                    TargetIp = Globals.AgentsLoadbalancer, // Use loadbalancer to route to agent
                    Bitstring = queries[item.Item2].query.BucketString,
                    HeadRouteID = ""
                };
                storeReq.Vector.AddRange(queries[item.Item2].query.Vector);
                storeReq.Chunk = item.Item1.Chunk;
                
                var storeRes = Globals.svec.Store(storeReq);
                Console.WriteLine($"[SearchAll] Stored chunk via Agent, id: {storeRes.Id}");
            }
            catch (Exception storeEx)
            {
                Console.WriteLine($"[SearchAll] Failed to store chunk via Agent: {storeEx.Message}");
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
        try
        {
            await foreach (var queryObj in requestStream.ReadAllAsync(context.CancellationToken))
            {
                try
                {
                    // Get neighboring buckets for this query
                    List<string> buckets = GetNeighbouringBuckets(queryObj.BucketString);
                    
                    // Create search request
                    SearchVector_Req req = new SearchVector_Req()
                    {
                        Index = queryObj.Index,
                        MinimumSimilarity = Globals.MinThresh,
                        K = Globals.K
                    };
                    req.Vector.AddRange(queryObj.Vector);
                    req.Bitstrings.AddRange(buckets);
                    
                    // Search in agent
                    SearchVector_Result res = await Globals.svs.ClientGet(req, Globals.AgentsLoadbalancer, "5000", context.CancellationToken);
                    
                    QueryResponseObject responseObj;
                    
                    if (res.Save)
                    {
                        // New chunk needs to be saved - call Agent to store it
                        SearchVectorObject svr = res.Results[0];
                        svr.Chunk = queryObj.Chunk; // Use original chunk
                        
                        // Store via Agent's StoreVector service (Agent will store locally)
                        StoreVector_Req storeReq = new StoreVector_Req
                        {
                            TargetIp = Globals.AgentsLoadbalancer, // Use loadbalancer to route to agent
                            Bitstring = queryObj.BucketString,
                            HeadRouteID = ""
                        };
                        storeReq.Vector.AddRange(queryObj.Vector);
                        storeReq.Chunk = queryObj.Chunk;
                        
                        try
                        {
                            Console.WriteLine($"[SearchAllStream] Preparing to store chunk - Bitstring: {queryObj.BucketString}, Chunk size: {queryObj.Chunk?.Length ?? 0}, Vector size: {queryObj.Vector?.Count ?? 0}");
                            var callOptions = new CallOptions(cancellationToken: context.CancellationToken);
                            var storeRes = Globals.svec.Store(storeReq, callOptions);
                            Console.WriteLine($"[SearchAllStream] ✅ SUCCESS: Stored chunk via Agent at {Globals.AgentsLoadbalancer}, id: {storeRes.Id}, index: {storeRes.Index}");
                        }
                        catch (Exception storeEx)
                        {
                            Console.WriteLine($"[SearchAllStream] ❌ ERROR: Failed to store chunk via Agent: {storeEx.Message}");
                            Console.WriteLine($"[SearchAllStream] ❌ Stack trace: {storeEx.StackTrace}");
                            // Continue anyway - chunk metadata might already be stored
                        }
                        
                        responseObj = new QueryResponseObject()
                        {
                            BucketId = svr.BucketId,
                            BucketKey = (ulong)svr.BucketKey,
                            Similarity = svr.Similarity,
                            Chunk = svr.Chunk,
                            Index = svr.Index,
                            Duplicate = true
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
                    }
                    
                    // Stream result back immediately
                    await responseStream.WriteAsync(responseObj);
                }
                catch (Exception ex)
                {
                    // Log error but continue processing other queries
                    Console.WriteLine($"[SearchAllStream] Error processing query {queryObj.Index}: {ex.Message}");
                    // Optionally send error response or skip this query
                }
            }
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
            List<string> neighbouringBuckets = GetNeighbouringBuckets(req.BucketString);
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
