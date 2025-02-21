
using Gateway.Modules.Agneta;
using Gateway.Utils.Globals;
using Grpc.Core;
using Grpc.Net.Client;

namespace Gateway.Services.Grpc;

public class SearchVectorService : SearchVector.SearchVectorClient
{
    public async Task<SearchVector_Result> ClientGet(SearchVector_Req req, string _ip)
    {
        try
        {
            var channel = GrpcChannel.ForAddress(Globals.AgentsLoadbalancer);
            StoreVector.StoreVectorClient _client = new StoreVector.StoreVectorClient(channel);

            return await _client.GetAsync(req);
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

