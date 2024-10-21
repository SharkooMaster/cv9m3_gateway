using Grpc.Core;
using GatewayClientServices;
using TpInternalService;
using Grpc.Net.Client;
using Gateway.Utils.Globals;

namespace Gateway.Services.Grpc;

public class BatchSearchService : Client_BatchSearchService.Client_BatchSearchServiceBase
{
    private readonly ILogger<BatchSearchService> _logger;

    public BatchSearchService(ILogger<BatchSearchService> logger)
    {
        _logger = logger;
    }

    public override Task<ByteArrayResponse> ProcessBytes(ByteArrayRequest request, ServerCallContext context)
    {
    }
}