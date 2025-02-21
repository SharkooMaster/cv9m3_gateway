
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
        await AgnetaHandler.Log(0, "Request recieved [SEARCH_ALL]");
        QueryResponse to_return = new QueryResponse();

        Parallel.For(0, request.QueryObjects.Count, async i => {
        // for (int i = 0; i < request.QueryObjects.Count; i++){
            SearchVector_Req _req = new SearchVector_Req();
            _req.Vector.AddRange(request.QueryObjects[i].Vector);
            _req.Bitstring = request.QueryObjects[i].BucketString;
            _req.K = Globals.K;
            _req.MinimumSimilarity = Globals.MinThresh;
            await AgnetaHandler.Log(0, request.QueryObjects[i].BucketString);

            //SearchVectorService
            await AgnetaHandler.Log(0, $"Searching agents [{i}]");
            SearchVector_Result res = await svs.ClientGet(_req, Globals.AgentsLoadbalancer);
            await AgnetaHandler.Log(0, $"Searched agents [{i}]::{res.Results.Count}");
            if(res.Results.Count == 0)
            {
                await AgnetaHandler.Log(0, $"Storing [{i}]");
                // save
                StoreVector_Req svecReq = new StoreVector_Req();
                svecReq.TargetIp = res.TargetIp;
                svecReq.Bitstring = _req.Bitstring;
                svecReq.Vector.AddRange(_req.Vector);

                M_Meta _meta = new M_Meta();
                _meta.chunk = Convert.ToBase64String(request.QueryObjects[i].Chunk.ToByteArray());
                svecReq.Metadata = JsonConvert.SerializeObject(_meta);
                await AgnetaHandler.Log(0, $"[{i}] Storing new vector");
                ulong vectorIndex = svec.Store(svecReq).Id;
                await AgnetaHandler.Log(0, $"[{i}] Stored new vector: {svecReq.Metadata[..20]}");

                to_return.Results.Add(new QueryResponseObject() {
                    Id = Convert.ToUInt64(_req.Bitstring, 2),
                    IdPost = vectorIndex,
                    Index = (uint)i,
                    Similarity = 1,
                    Chunk = request.QueryObjects[i].Chunk
                });
            }
            else
            {
                for (int j = 0; j < res.Results.Count; j++)
                {
                    await AgnetaHandler.Log(0, $"[{i}]:[{j}] Sim: {res.Results[j].SimilarityRate}");
                    if(res.Results[j].SimilarityRate >= Globals.MinThresh)
                    {
                        await AgnetaHandler.Log(0, $"[{i}]:[{j}] Match found, preparing QRO");
                        JObject _meta = JObject.Parse(res.Results[j].Metadata);
                        await AgnetaHandler.Log(0, $"[{i}]:[{j}] Meta parsed");
                        Google.Protobuf.ByteString _chunk = ByteString.CopyFrom(Convert.FromBase64String(_meta["chunk"]?.ToString()));
                        await AgnetaHandler.Log(0, $"[{i}]:[{j}] Chunk copied");

                        to_return.Results.Add(new QueryResponseObject() {
                            Id = res.Results[j].Id,
                            IdPost = res.Results[j].Index,
                            Index = (uint)i,
                            Similarity = res.Results[j].SimilarityRate,
                            Chunk = _chunk
                        });
                        await AgnetaHandler.Log(0, $"[{i}]:[{j}] Result added");
                    }
                }
            }
        // }
        });

        return to_return;
    }
}
