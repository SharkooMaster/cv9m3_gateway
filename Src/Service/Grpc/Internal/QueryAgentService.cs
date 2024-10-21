using Gateway.Utils.Globals;
using Grpc.Net.Client;
using TpInternalService;

namespace Agent.Services.Grpc;

public class QueryAgentService
{
    private readonly QueryAgent.QueryAgentClient _client;
    private readonly ILogger<QueryAgentService> _logger;

    public QueryAgentService(ILogger<QueryAgentService> logger)
    {
        _logger = logger;

        var channel = GrpcChannel.ForAddress(Globals.AgentsLoadbalancer);
        _client = new QueryAgent.QueryAgentClient(channel);
    }

    public async Task<QueryResponse> QueryAsync(string key, float[] vectorData, int topK, float minThresh)
    {
        var vector = new Vector
        {
            Vec = { vectorData }
        };

        var request = new QueryRequest
        {
            Key = key,
            Vector = vector,
            TopK = topK,
            MinThresh = minThresh
        };

        var response = await _client.QueryAsync(request);

        return response;
    }

}