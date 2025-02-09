
using GatewayService;
using Grpc.Core;

namespace Gateway.Services.Grpc;

public class SearchAllService : GatewayService.GatewayService.GatewayServiceBase
{
    public override Task<QueryResponse> SearchAll(QueryRequest request, ServerCallContext context)
    {
        Parallel.For(0, request.QueryObjects.Count, i => {
            SearchVector_Req _req = new SearchVector_Req();
            _req.Vector.AddRange(request.QueryObjects[i].Vector);
            _req.Bitstring = request.QueryObjects[i].BucketString;
            _req.K = 4;
            _req.MinimumSimilarity = 0.6f;

            //SearchVectorService
        });
        return base.SearchAll(request, context);
    }
}
