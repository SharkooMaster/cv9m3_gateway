
using Gateway.Modules.Agneta;
using Gateway.Utils.Globals;
using Grpc.Core;
using Grpc.Net.Client;

namespace Gateway.Services.Grpc;

public class SearchVectorService : SearchVector.SearchVectorClient
{
    public async Task<SearchVector_Result> ClientGet(SearchVector_Req req, string _ip, string _port = "5000", CancellationToken ct = default)
    {
        try
        {
            var _client = GrpcChannelFactory.GetClient(
                target: _ip,
                ctor: chan => new SearchVector.SearchVectorClient(chan),
                roundRobin: (_port == "5000") ? false : true
            );

            var deadline = DateTime.UtcNow.AddSeconds(5);
            return await _client.GetAsync(req, deadline: deadline, cancellationToken: ct);
        }
        catch(RpcException ex)
        {
            await AgnetaHandler.Log(2, $"gRPC error: {ex.Status.StatusCode} - {ex.Status.Detail}");
            throw;
        }
        catch (Exception ex)
        {
            await AgnetaHandler.Log(2, $"[SearchVector] General error: {ex.Message}");
            throw;
        }
    }
}

