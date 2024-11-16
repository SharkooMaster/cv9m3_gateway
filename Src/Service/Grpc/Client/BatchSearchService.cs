using Grpc.Core;
using GatewayClientServices;
using TpInternalService;
using Grpc.Net.Client;
using Gateway.Utils.Globals;
using Gateway.Modules.Agneta;
using Agent.Services.Grpc;

namespace Gateway.Services.Grpc;

public class BatchSearchService : Client_BatchSearchService.Client_BatchSearchServiceBase
{
    private readonly ILogger<BatchSearchService> _logger;
    private readonly QueryAgentService queryAgentService;

    public BatchSearchService(ILogger<BatchSearchService> logger, QueryAgentService _queryAgentService)
    {
        _logger = logger;
        queryAgentService = _queryAgentService;
    }

    public override async Task<ByteArrayResponse> ProcessBytes(ByteArrayRequest request, ServerCallContext context)
    {
        await AgnetaHandler.Log(0, "Recieved request to BatchSearch");

        List<QueryResponse> responses = new List<QueryResponse>();

        List<Task> searchReqs = new List<Task>();
        for (int i = 0; i < request.Vectors.Count; i++)
        {
            searchReqs.Add(new Task(async () => {
                responses[i] = await queryAgentService.QueryAsync("", request.Vectors[i].Vec.ToArray(), request.TopK, Globals.MinThresh);
            }));
        }
        await Task.WhenAll(searchReqs);

        return null;
    }
}