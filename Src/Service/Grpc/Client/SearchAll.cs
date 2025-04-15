
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
    string headID = "";

    private async Task initCLMS(string _name, string _id)
    {
        await ClmsHandler.RegisterRoutePoint(headID, _name, _id);
    }

    private async Task addEvent(string _step, string _message, string _type = "step", string _level = "1")
    {
        await ClmsHandler.AddEventToRoutePoint(headID, new M_CLMSEvent(){
            level = _level, stepName = _step, type = _type, message = _message
        });
    }

    private List<string> GetNeighbouringBuckets(string _bitString)
    {
        // Generate bit-flipped variations
        List<string> bitFlippedStrings = new List<string>();
        for (int j = 0; j < Globals.K; j++)
        {
            char[] modifiedBits = _bitString.ToCharArray();
            modifiedBits[j] = (modifiedBits[j] == '0') ? '1' : '0';
            bitFlippedStrings.Add(new string(modifiedBits));
        }
        bitFlippedStrings.Add(_bitString);

        return bitFlippedStrings;
    }
    
    public override async Task<QueryResponse> SearchAll(QueryRequest request, ServerCallContext context)
    {
        try
        {
            headID = request.HeadRouteID;
            await initCLMS("Gateway", "A2");

            QueryResponse response = new QueryResponse();
            ConcurrentBag<QueryResponseObject> resultsBag = new ConcurrentBag<QueryResponseObject>();

            // get neighbour buckets
            QueryObject req = request.QueryObjects[0];
            List<string> neighbouringBuckets = GetNeighbouringBuckets(req.BucketString);
            ConcurrentBag<SearchVectorObject> searchResults = new ConcurrentBag<SearchVectorObject>();
            string targetIP = Globals.AgentsLoadbalancer;

            ParallelOptions options = new ParallelOptions() { MaxDegreeOfParallelism = 4 };
            await Parallel.ForAsync(0, neighbouringBuckets.Count, options, async (i, ct) => {
                await addEvent("SearchAll:Start", $"Preparing search_vector_request {i}" );

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
                        await addEvent("SearchAll:Searching", $"Routing search to {route_ip} | {neighbouringBuckets[i]}");
                        _res = await Globals.svs.ClientGet(searchReq, route_ip);

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
                    await addEvent("SearchAll:FailedToSearch", $"Request to search for vector failed: {ex.Message} : {ex.Data}");
                    await ClmsHandler.SendRoutePoint(headID);
                    throw;
                }
            });

            if(searchResults.Count == 0)
            {
                await addEvent("Searched:Nothing Found", "No result found. Saving");
                StoreVector_Req svecReq = new StoreVector_Req
                {
                    TargetIp = targetIP,
                    Bitstring = req.BucketString,
                    HeadRouteID = headID
                };
                svecReq.Vector.AddRange(req.Vector);

                M_Meta meta = new M_Meta
                {
                    chunk = Convert.ToBase64String(req.Chunk.ToByteArray())
                };
                svecReq.Metadata = JsonConvert.SerializeObject(meta);

                await addEvent("Searched:Storing", $"Sending results to save");
                ulong vectorIndex = Globals.svec.Store(svecReq).Id;
                await addEvent("Searched:Storing", $"Saved results");

                resultsBag.Add(new QueryResponseObject
                {
                    Id = Convert.ToUInt64(req.BucketString, 2),
                    IdPost = vectorIndex,
                    Index = req.Index,
                    Similarity = 1,
                    Chunk = req.Chunk
                });
            }
            else
            {
                await addEvent("Searched:Found", "Result found");
                foreach (var result in searchResults)
                {
                    if (result.SimilarityRate >= Globals.MinThresh)
                    {
                        JObject meta = JObject.Parse(result.Metadata);
                        Google.Protobuf.ByteString chunk = ByteString.CopyFrom(
                            Convert.FromBase64String(meta["chunk"]?.ToString()));

                        resultsBag.Add(new QueryResponseObject
                        {
                            Id = result.Id,
                            IdPost = result.Index,
                            Index = request.QueryObjects[0].Index,
                            Similarity = result.SimilarityRate,
                            Chunk = chunk
                        });
                    }
                }
            }

            await addEvent("SearchAll:Final", "Done");
            await ClmsHandler.SendRoutePoint(headID);

            response.Results.AddRange(resultsBag);
            return response;
        }
        catch(Exception exc)
        {
            Console.WriteLine(exc);
            PushoverHandler.PushNotification($"Error, gateway process failed: {exc.Data} | {exc.Message}");
        }
    }

}
