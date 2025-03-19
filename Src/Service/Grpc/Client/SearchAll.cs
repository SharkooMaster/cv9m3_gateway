
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.Json;
using Gateway.Modules.Agneta;
using Gateway.Utils.Globals;
using Gateway.Utils.Misc;
using GatewayService;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.AspNetCore.Server.HttpSys;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit.Sdk;

namespace Gateway.Services.Grpc;

public class SearchAllService : GatewayService.GatewayService.GatewayServiceBase
{
    public override async Task<QueryResponse> SearchAll(QueryRequest request, ServerCallContext context)
    {
        QueryResponse response = new QueryResponse();
        //List<QueryResponseObject> resultsBag = new List<QueryResponseObject>();
        ConcurrentBag<QueryResponseObject> resultsBag = new ConcurrentBag<QueryResponseObject>();

        // Create a list of tasks to execute in parallel
        var searchTasks = request.QueryObjects.Select(async (queryObj, index) =>
        {
            SearchVector_Req req = new SearchVector_Req
            {
                Bitstring = queryObj.BucketString,
                K = Globals.K,
                MinimumSimilarity = Globals.MinThresh
            };
            req.Vector.AddRange(queryObj.Vector);

            // Generate bit-flipped variations
            List<string> bitFlippedStrings = new List<string>();
            for (int j = 0; j < 64; j++)
            {
                char[] modifiedBits = req.Bitstring.ToCharArray();
                modifiedBits[j] = (modifiedBits[j] == '0') ? '1' : '0';
                bitFlippedStrings.Add(new string(modifiedBits));
            }

            if (!bitFlippedStrings.Contains(req.Bitstring))
            {
                bitFlippedStrings.Add(req.Bitstring);
            }

            Stopwatch sw = Stopwatch.StartNew();
            string _target_ip = "";
            ConcurrentBag<SearchVector_Result> searchResults = new ConcurrentBag<SearchVector_Result>();

            // Run parallel searches properly
            var searchVectorTasks = bitFlippedStrings.Select(async flippedBitstring =>
            {
                SearchVector_Req searchReq = new SearchVector_Req
                {
                    Bitstring = flippedBitstring,
                    K = Globals.K,
                    MinimumSimilarity = Globals.MinThresh
                };
                searchReq.Vector.AddRange(req.Vector);

                SearchVector_Result _res = await Globals.svs.ClientGet(searchReq, Globals.AgentsLoadbalancer);
                    searchResults.Add(_res);

                if (searchReq.Bitstring == req.Bitstring)
                {
                    _target_ip = _res.TargetIp;
                }
            }).ToList();

            await Task.WhenAll(searchVectorTasks); // Properly await parallel searches

            SearchVector_Result res = new SearchVector_Result();
            foreach (var searchResult in searchResults)
            {
                res.Results.AddRange(searchResult.Results);
            }
            res.TargetIp = _target_ip;

            sw.Stop();
            Console.WriteLine($"{index}: took {sw.ElapsedMilliseconds}ms to search for buckets");

            if (res.Results.Count == 0)
            {
                Console.WriteLine("res.res.cnt 0");
                // Save
                StoreVector_Req svecReq = new StoreVector_Req
                {
                    TargetIp = res.TargetIp,
                    Bitstring = req.Bitstring
                };
                svecReq.Vector.AddRange(req.Vector);

                M_Meta meta = new M_Meta
                {
                    chunk = Convert.ToBase64String(queryObj.Chunk.ToByteArray())
                };
                svecReq.Metadata = JsonConvert.SerializeObject(meta);
                ulong vectorIndex = Globals.svec.Store(svecReq).Id;

                resultsBag.Add(new QueryResponseObject
                {
                    Id = Convert.ToUInt64(req.Bitstring, 2),
                    IdPost = vectorIndex,
                    Index = queryObj.Index,
                    Similarity = 1,
                    Chunk = queryObj.Chunk
                });
            }
            else
            {
                Console.WriteLine("res.res.cnt more than 0");
                foreach (var result in res.Results)
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
                            Index = queryObj.Index,
                            Similarity = result.SimilarityRate,
                            Chunk = chunk
                        });
                    }
                }
            }
        }).ToList();

        await Task.WhenAll(searchTasks); // Wait for all queries to finish

        response.Results.AddRange(resultsBag);
        return response;
    }


    //public override async Task<QueryResponse> SearchAll(QueryRequest request, ServerCallContext context)
    //{
    //    // await AgnetaHandler.Log(0, "Request received [SEARCH_ALL]");
    //    QueryResponse response = new QueryResponse();
    //    ConcurrentBag<QueryResponseObject> resultsBag = new ConcurrentBag<QueryResponseObject>();
    //    ConcurrentBag<int> FoundBag = new ConcurrentBag<int>();

    //    // Create a list of tasks instead of using Parallel.For with async lambdas

    //    Parallel.For(0, request.QueryObjects.Count, async i => {
    //        int index = i; // capture the loop variable
    //        SearchVector_Req req = new SearchVector_Req();
    //        req.Vector.AddRange(request.QueryObjects[index].Vector);
    //        req.Bitstring = request.QueryObjects[index].BucketString;
    //        req.K = Globals.K;
    //        req.MinimumSimilarity = Globals.MinThresh;

    //        List<string> bitFlippedStrings = new List<string>();
    //        for (int j = 0; j < 64; j++)
    //        {
    //            char[] modifiedBits = req.Bitstring.ToCharArray();
    //            modifiedBits[j] = (modifiedBits[j] == '0') ? '1' : '0';
    //            bitFlippedStrings.Add(new string(modifiedBits));
    //        }

    //        if(!bitFlippedStrings.Contains(req.Bitstring))
    //        {
    //            bitFlippedStrings.Add(req.Bitstring);
    //        }

    //        Stopwatch sw = new Stopwatch();
    //        sw.Start();

    //        string _target_ip = "";
    //        ConcurrentBag<SearchVector_Result> searchResults = new ConcurrentBag<SearchVector_Result>();
    //        await Parallel.ForEachAsync(bitFlippedStrings, async (flippedBitstring, token) =>
    //        {
    //            SearchVector_Req searchReq = new SearchVector_Req
    //            {
    //                Bitstring = flippedBitstring,
    //                Vector = { req.Vector },
    //                K = Globals.K,
    //                MinimumSimilarity = Globals.MinThresh
    //            };

    //            SearchVector_Result _res = await svs.ClientGet(searchReq, Globals.AgentsLoadbalancer);
    //            searchResults.Add(_res);

    //            if(searchReq.Bitstring == req.Bitstring){ _target_ip = _res.TargetIp; }
    //        });

    //        SearchVector_Result res = new SearchVector_Result();
    //        List<SearchVector_Result> _searchResults = searchResults.ToList();
    //        for (int k = 0; k < searchResults.Count; k++)
    //        {
    //            res.Results.AddRange(_searchResults[k].Results);
    //        }
    //        res.TargetIp = _target_ip;
    //        sw.Stop();
    //        Console.WriteLine($"{index}: took {sw.ElapsedMilliseconds}ms to search for buckets");

    //        if (res.Results.Count == 0)
    //        {
    //            // Save
    //            StoreVector_Req svecReq = new StoreVector_Req
    //            {
    //                TargetIp = res.TargetIp,
    //                Bitstring = req.Bitstring
    //            };
    //            svecReq.Vector.AddRange(req.Vector);

    //            M_Meta meta = new M_Meta
    //            {
    //                chunk = Convert.ToBase64String(request.QueryObjects[index].Chunk.ToByteArray())
    //            };
    //            svecReq.Metadata = JsonConvert.SerializeObject(meta);
    //            svecReq.TargetIp = res.TargetIp;
    //            ulong vectorIndex = svec.Store(svecReq).Id;

    //            resultsBag.Add(new QueryResponseObject
    //            {
    //                Id = Convert.ToUInt64(req.Bitstring, 2),
    //                IdPost = vectorIndex,
    //                Index = request.QueryObjects[index].Index,
    //                Similarity = 1,
    //                Chunk = request.QueryObjects[index].Chunk
    //            });
    //        }
    //        else
    //        {
    //            for (int j = 0; j < res.Results.Count; j++)
    //            {
    //                if (res.Results[j].SimilarityRate >= Globals.MinThresh)
    //                {
    //                    JObject meta = JObject.Parse(res.Results[j].Metadata);
    //                    Google.Protobuf.ByteString chunk = ByteString.CopyFrom(
    //                    Convert.FromBase64String(meta["chunk"]?.ToString()));

    //                    resultsBag.Add(new QueryResponseObject
    //                    {
    //                        Id = res.Results[j].Id,
    //                        IdPost = res.Results[j].Index,
    //                        Index = request.QueryObjects[index].Index,
    //                        Similarity = res.Results[j].SimilarityRate,
    //                        Chunk = chunk
    //                    });
    //                }
    //            }
    //        }
    //    });

    //    response.Results.AddRange(resultsBag);
    //    return response;
    //}
}
