
using System.Collections.Concurrent;
using System.Text.Json;
using Gateway.Modules.Agneta;
using Gateway.Utils.Globals;
using Gateway.Utils.Misc;
using GatewayService;
using Google.Protobuf;
using Grpc.Core;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Gateway.Services.Grpc;

public class SearchAllService : GatewayService.GatewayService.GatewayServiceBase
{
    SearchVectorService svs = new SearchVectorService();
    StoreVectorService svec = new StoreVectorService();

    public override async Task<QueryResponse> SearchAll(QueryRequest request, ServerCallContext context)
    {
        // await AgnetaHandler.Log(0, "Request received [SEARCH_ALL]");
        QueryResponse response = new QueryResponse();
        ConcurrentBag<QueryResponseObject> resultsBag = new ConcurrentBag<QueryResponseObject>();
        ConcurrentBag<int> FoundBag = new ConcurrentBag<int>();

        // Create a list of tasks instead of using Parallel.For with async lambdas
        var tasks = new List<Task>();

        for (int i = 0; i < request.QueryObjects.Count; i++)
        {
            int index = i; // capture the loop variable
            tasks.Add(Task.Run(async () =>
            {
                SearchVector_Req req = new SearchVector_Req();
                req.Vector.AddRange(request.QueryObjects[index].Vector);
                req.Bitstring = request.QueryObjects[index].BucketString;
                req.K = Globals.K;
                req.MinimumSimilarity = Globals.MinThresh;
                await AgnetaHandler.Log(0, request.QueryObjects[index].BucketString);
                char[] req_bitstring = req.Bitstring.ToCharArray();

                SearchVector_Result res = new SearchVector_Result();
                for (int j = 0; j < 64; j++)
                {
                    if(j > 0)
                    {
                        req_bitstring[j] = (req_bitstring[j] == '0') ? '1' : '0';
                        req.Bitstring = new string(req_bitstring);
                    }
                    await AgnetaHandler.Log(0, $"Searching agents [{index}]");
                    SearchVector_Result _res = await svs.ClientGet(req, Globals.AgentsLoadbalancer);
                    await AgnetaHandler.Log(0, $"Searched agents [{index}]::{_res.Results.Count}");

                    res.Results.AddRange(_res.Results);
                    if(j == 0)
                    {
                        res.TargetIp = _res.TargetIp;
                    }
                }

                if (res.Results.Count == 0 && !request.QueryObjects[index].IsNeighbour)
                {
                    await AgnetaHandler.Log(0, $"Storing [{index}]");
                    // Save
                    StoreVector_Req svecReq = new StoreVector_Req
                    {
                        TargetIp = res.TargetIp,
                        Bitstring = req.Bitstring
                    };
                    svecReq.Vector.AddRange(req.Vector);

                    M_Meta meta = new M_Meta
                    {
                        chunk = Convert.ToBase64String(request.QueryObjects[index].Chunk.ToByteArray())
                    };
                    svecReq.Metadata = JsonConvert.SerializeObject(meta);
                    await AgnetaHandler.Log(0, $"[{index}] Storing new vector");
                    ulong vectorIndex = svec.Store(svecReq).Id;
                    await AgnetaHandler.Log(0, $"[{index}] Stored new vector: {svecReq.Metadata[..20]}");

                    resultsBag.Add(new QueryResponseObject
                    {
                        Id = Convert.ToUInt64(req.Bitstring, 2),
                        IdPost = vectorIndex,
                        Index = request.QueryObjects[index].Index,
                        Similarity = 1,
                        Chunk = request.QueryObjects[index].Chunk
                    });
                }
                else
                {
                    for (int j = 0; j < res.Results.Count; j++)
                    {
                        await AgnetaHandler.Log(0, $"[{index}]:[{j}] Sim: {res.Results[j].SimilarityRate}");
                        if (res.Results[j].SimilarityRate >= Globals.MinThresh)
                        {
                            await AgnetaHandler.Log(0, $"[{index}]:[{j}] Match found, preparing QRO");
                            JObject meta = JObject.Parse(res.Results[j].Metadata);
                            await AgnetaHandler.Log(0, $"[{index}]:[{j}] Meta parsed");
                            Google.Protobuf.ByteString chunk = ByteString.CopyFrom(
                                Convert.FromBase64String(meta["chunk"]?.ToString()));
                            await AgnetaHandler.Log(0, $"[{index}]:[{j}] Chunk copied");

                            resultsBag.Add(new QueryResponseObject
                            {
                                Id = res.Results[j].Id,
                                IdPost = res.Results[j].Index,
                                Index = request.QueryObjects[index].Index,
                                Similarity = res.Results[j].SimilarityRate,
                                Chunk = chunk
                            });
                            await AgnetaHandler.Log(0, $"[{index}]:[{j}] Result added {resultsBag.Count}");
                        }
                    }
                }
            }));
        }

        // Await all the tasks to complete
        await Task.WhenAll(tasks);

        response.Results.AddRange(resultsBag);
        return response;
    }
}
