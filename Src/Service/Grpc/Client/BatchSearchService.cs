using Grpc.Core;
using GatewayClientServices;
using TpInternalService;
using Grpc.Net.Client;
using Gateway.Utils.Globals;
using Gateway.Modules.Agneta;

namespace Gateway.Services.Grpc;

public class BatchSearchService : Client_BatchSearchService.Client_BatchSearchServiceBase
{
    private readonly ILogger<BatchSearchService> _logger;

    public BatchSearchService(ILogger<BatchSearchService> logger)
    {
        _logger = logger;
    }

    public override async Task<ByteArrayResponse> ProcessBytes(ByteArrayRequest request, ServerCallContext context)
    {
        Console.WriteLine("here");
        await AgnetaHandler.Log(0, "Recieved request to BatchSearch");
        return null;
    }
}