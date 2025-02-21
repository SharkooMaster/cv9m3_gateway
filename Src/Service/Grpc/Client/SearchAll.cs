
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

    public override async Task<QueryResponse> SearchAll(QueryRequest request, ServerCallContext context)
    {
        await AgnetaHandler.Log(0, "Request recieved [SEARCH_ALL]");
        QueryResponse to_return = new QueryResponse();

        Parallel.For(0, request.QueryObjects.Count, async i => {
            SearchVector_Req _req = new SearchVector_Req();
            _req.Vector.AddRange(request.QueryObjects[i].Vector);
            _req.Bitstring = request.QueryObjects[i].BucketString;
            _req.K = Globals.K;
            _req.MinimumSimilarity = Globals.MinThresh;
            await AgnetaHandler.Log(0, request.QueryObjects[i].BucketString);

            //SearchVectorService
            await AgnetaHandler.Log(0, $"Searching agents [{i}]");
            SearchVector_Result res = await svs.ClientGet(_req, Globals.AgentsLoadbalancer);
            await AgnetaHandler.Log(0, $"Searched agents [{i}]");
            for (int j = 0; j < res.Results.Count; j++)
            {
                await AgnetaHandler.Log(0, $"[{i}]:[{j}] Sim: {res.Results[j].SimilarityRate}");
                if(res.Results[j].SimilarityRate >= Globals.MinThresh)
                {
                    JObject _meta = JObject.Parse(res.Results[j].Metadata);
                    Google.Protobuf.ByteString _chunk = ByteString.CopyFrom(Gateway.Utils.Misc.Misc.HexStringToByteArray(_meta["chunk"]?.ToString()));

                    to_return.Results.Add(new QueryResponseObject() {
                        Id = res.Results[j].Id,
                        IdPost = res.Results[j].Index,
                        Index = (uint)i,
                        Similarity = res.Results[j].SimilarityRate,
                        Chunk = _chunk
                    });
                }
            }
        });

        return to_return;
    }
}
