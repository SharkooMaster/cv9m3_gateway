using Gateway.Utils.Globals;
using Google.Protobuf.Collections;
using Grpc.Core;
using Grpc.Net.Client;
using TpInternalService;

namespace Gateway.Services.Grpc;

public class StoreBucketRowAgentService
{
    private readonly StoreBucketRowAgent.StoreBucketRowAgentClient _client;
    private readonly ILogger<QueryAgentService> _logger;

    public StoreBucketRowAgentService(ILogger<QueryAgentService> logger)
    {
        _logger = logger;
        var channel = GrpcChannel.ForAddress(Globals.AgentsLoadbalancer, Globals.GRPC_OPTIONS);
        _client = new StoreBucketRowAgent.StoreBucketRowAgentClient(channel);
    }

    public async Task<bool> StoreBucketRowAsync(long id, float[] vectorData, byte[] chunkData, string key)
    {
        var vectorArr = new VectorArr
        {
            Vec = { vectorData }
        };

        var request = new StoreRequest
        {
            Id = id,
            Vector = vectorArr,
            Chunk = Google.Protobuf.ByteString.CopyFrom(chunkData),
            Key = key
        };

        var response = await _client.StoreBucketRowAsync(request);

        return response.Status;
    }
}