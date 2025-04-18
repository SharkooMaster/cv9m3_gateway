
using Gateway.Modules.Agneta;
using Gateway.Utils.Globals;
using Grpc.Core;
using Grpc.Net.Client;

namespace Gateway.Services.Grpc;

public class SearchVectorService : SearchVector.SearchVectorClient
{
    public async Task<SearchVector_Result> ClientGet(SearchVector_Req req, string _ip, CancellationToken ct = default)
    {
        try
        {
            GrpcChannel? channel = null;
            if(_ip == Globals.AgentsLoadbalancer)
            {
                channel = GrpcChannelFactory.GetChannel(_ip, ":80");
            }
            else
            {
                channel = GrpcChannelFactory.GetChannel(_ip);
            }

            SearchVector.SearchVectorClient _client = new SearchVector.SearchVectorClient(channel);

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

