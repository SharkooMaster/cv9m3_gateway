using Grpc.Core;

namespace Gateway.Services.Grpc;

public class BatchSearchService
{
    private readonly ILogger<GreeterService> _logger;

    public BatchSearchService(ILogger<GreeterService> logger)
    {
        _logger = logger;
    }
}