
using System.Collections.Concurrent;
using System.Diagnostics;
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

namespace Gateway.Services.Grpc;

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

        for (int i = 0; i < Globals.K; i++)
        {
            char original = buffer[i];
            buffer[i] = (original == '0') ? '1' : '0';
            results.Add(new string(buffer));
            buffer[i] = original; // restore original bit
        }

        results.Add(bitString);
        return results;
    }

    public override async Task<QueryResponse> SearchAll(QueryRequest request, ServerCallContext context)
    {
        QueryResponse response = new QueryResponse();
        
        SearchVector_Reqs outgoingBatch = new SearchVector_Reqs();
        Dictionary<int, QueryObject> indexedChunks = new Dictionary<int, QueryObject>();

        for (int j = 0; j < request.QueryObjects.Count; j++)
        {
            QueryObject req = request.QueryObjects[j];
            indexedChunks.Add(req.Index, req);
            List<string> neighbouringBuckets = GetNeighbouringBuckets(req.BucketString);

            for (int i = 0; i < neighbouringBuckets.Count; i++)
            {
                SearchVector_Req searchReq = new SearchVector_Req
                {
                    Bitstring = neighbouringBuckets[i],
                    K = Globals.K,
                    MinimumSimilarity = Globals.MinThresh,
                    HeadRouteID = "",
                    Index = req.Index
                };
                searchReq.Vector.AddRange(req.Vector);
                outgoingBatch.Reqs.Add(searchReq);
            }
        }
        
        Stopwatch sw = new Stopwatch();
        sw.Start();

        SearchVector_Results results = await Globals.svs.ClientGet(
            outgoingBatch,
            Globals.AgentsLoadbalancer,
            "80",
            context.CancellationToken
        );
        sw.Stop();
        Console.WriteLine($"Time to search {sw.ElapsedMilliseconds}ms");

        List<QueryResponseObject> resultsBag = new List<QueryResponseObject>();
        List<SearchVector_Result> ToStore = new List<SearchVector_Result>();
        for (int i = 0; i < results.Results.Count; i++)
        {
            SearchVector_Result current_result = results.Results[i];
            if(current_result.Save)
            {
                Console.WriteLine("Saving 1");
                ToStore.Add(current_result);

                resultsBag.Add(new QueryResponseObject
                {
                    Id = current_result.Results[0].Id,
                    Similarity = 1,
                    Chunk = indexedChunks[current_result.Results[0].I].Chunk,
                    Index = current_result.Results[0].Index,
                    I = current_result.Results[0].I
                });
            }
            else
            {
                foreach (var result in current_result.Results)
                {
                    if (result.SimilarityRate >= Globals.MinThresh)
                    {
                        Console.WriteLine($"found {result.SimilarityRate}");
                        resultsBag.Add(new QueryResponseObject
                        {
                            Id = result.Id,
                            Index = current_result.Results[0].Index,
                            Similarity = result.SimilarityRate,
                            Chunk = result.Chunk,
                            I = current_result.Results[0].I
                        });
                    }
                    else
                    {
                        Console.WriteLine("Saving 2");
                        ToStore.Add(current_result);

                        resultsBag.Add(new QueryResponseObject
                        {
                            Id = current_result.Results[0].Id,
                            Similarity = 1,
                            Chunk = indexedChunks[current_result.Results[0].I].Chunk,
                            Index = current_result.Results[0].Index,
                            I = current_result.Results[0].I
                        });
                    }
                }
            }
        }
        response.Results.AddRange(resultsBag);

        // Store
        for (int i = 0; i < ToStore.Count; i++)
        {
            byte[] _chunk = indexedChunks[ToStore[i].Results[0].Index].Chunk.ToArray();
            float[] _vec = indexedChunks[ToStore[i].Results[0].Index].Vector.ToArray();

            _ = NetworkFileStorageHandler.StoreVector("", new M_Data() {
                chunk = _chunk,
                vector = _vec
            });
        }
        return response;
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
