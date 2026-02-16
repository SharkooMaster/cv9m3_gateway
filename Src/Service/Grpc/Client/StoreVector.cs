
using Gateway.Modules.Agneta;
using Gateway.Utils.Globals;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;

namespace Gateway.Services.Grpc;

public class StoreVectorService : StoreVector.StoreVectorClient
{
    private static int GetStoreTimeoutSeconds()
    {
        var raw = Environment.GetEnvironmentVariable("GATEWAY_STORE_TIMEOUT_SEC");
        if (int.TryParse(raw, out var sec) && sec > 0)
            return sec;
        return 30;
    }

    public override StoreVector_Res Store(StoreVector_Req request, CallOptions options)
    {
        try
        {
            var _client = GrpcChannelFactory.GetClient(
                target: request.TargetIp,
                ctor: chan => new StoreVector.StoreVectorClient(chan),
                roundRobin: false
            );

            var deadline = DateTime.UtcNow.AddSeconds(GetStoreTimeoutSeconds());
            return _client.Store(request, deadline: deadline);
        }
        catch(RpcException ex)
        {
            AgnetaHandler.Log(2, $"gRPC error: {ex.Status.StatusCode} - {ex.Status.Detail}");
            throw;
        }
        catch (Exception ex)
        {
            AgnetaHandler.Log(2, $"[SearchVector] General error: {ex.Message}");
            throw;
        }
    }
}
