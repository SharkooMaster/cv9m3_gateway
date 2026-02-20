
using Gateway.Modules.Agneta;
using Gateway.Utils;
using Gateway.Utils.Globals;
using Grpc.Core;
using Grpc.Net.Client;

namespace Gateway.Services.Grpc;

public class SearchVectorService : SearchVector.SearchVectorClient
{
    private static bool IsIpv4Literal(string target)
        => System.Net.IPAddress.TryParse(target, out var ip)
           && ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork;

    private static int GetSearchTimeoutSeconds()
    {
        var raw = Environment.GetEnvironmentVariable("GATEWAY_SEARCH_TIMEOUT_SEC");
        if (int.TryParse(raw, out var sec) && sec > 0)
            return sec;
        return 30; // Allow more time for large batch requests
    }

    public async Task<SearchVector_Result> ClientGet(SearchVector_Req req, string _ip, string _port = "5000", CancellationToken ct = default)
    {
        try
        {
            var _client = GrpcChannelFactory.GetClient(
                target: _ip,
                ctor: chan => new SearchVector.SearchVectorClient(chan),
                roundRobin: LocalModeDetector.IsLocalMode() && !IsIpv4Literal(_ip)
            );

            var deadline = DateTime.UtcNow.AddSeconds(GetSearchTimeoutSeconds());
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

    /// <summary>
    /// Send a batch of queries to a single agent in ONE gRPC call.
    /// Returns one SearchVector_Result per query, in the same order.
    /// </summary>
    public async Task<BatchSearchVector_Result> ClientBatchGet(BatchSearchVector_Req req, string agentIp, CancellationToken ct = default)
    {
        var _client = GrpcChannelFactory.GetClient(
            target: agentIp,
            ctor: chan => new SearchVector.SearchVectorClient(chan),
            roundRobin: false // Always direct to specific agent IP
        );

        var deadline = DateTime.UtcNow.AddSeconds(GetSearchTimeoutSeconds());
        return await _client.BatchGetAsync(req, deadline: deadline, cancellationToken: ct);
    }
}

