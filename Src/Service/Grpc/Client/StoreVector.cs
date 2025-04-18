
using Gateway.Modules.Agneta;
using Gateway.Utils.Globals;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;

namespace Gateway.Services.Grpc;

public class StoreVectorService : StoreVector.StoreVectorClient
{
    public override StoreVector_Res Store(StoreVector_Req request, CallOptions options)
    {
        try
        {
            var _client = GrpcChannelFactory.GetClient(ip: request.TargetIp, chan => new StoreVector.StoreVectorClient(chan), port: "5000");

            var deadline = DateTime.UtcNow.AddSeconds(5);
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
