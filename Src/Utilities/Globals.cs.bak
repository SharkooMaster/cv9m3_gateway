using Gateway.Models;
using Gateway.Services.Grpc;
using Gateway.Utils.Misc;
using Grpc.Net.Client;
using Grpc.Net.Client.Configuration;
namespace Gateway.Utils.Globals;

public static class Globals
{
    // ETCD_CONFIG
    //public static string ETCD_ID = "";
    //public static string ETCD_VALUE = "";
    //public static long ETCD_LEASE_ID = -1;

    public static string GATEWAY_ID         = Misc.Misc.GenerateId();
    public static string GATEWAY_VALUE      = "";
    public static long   GATEWAY_LEASE_ID   = -1;

    public static float MinThresh = 0.6f;
    public static int K = 2;

    // AGENTS_CONFIG
    public static string AgentsLoadbalancer = "agent-headless.cross-test.svc.cluster.local";
    public static SearchVectorService svs = new SearchVectorService();
    public static StoreVectorService svec = new StoreVectorService();

    public static GrpcChannelOptions GRPC_OPTIONS = new GrpcChannelOptions{
        HttpHandler = new SocketsHttpHandler() {
            EnableMultipleHttp2Connections = true,
            PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
            KeepAlivePingDelay = TimeSpan.FromSeconds(30),
            KeepAlivePingTimeout = TimeSpan.FromSeconds(10)
        },
/*         LoggerFactory = LoggerFactory.Create(lb =>
        {
            lb.AddConsole();
            lb.SetMinimumLevel(LogLevel.Debug);
        }), */
        ServiceConfig = new Grpc.Net.Client.Configuration.ServiceConfig()
        {
            MethodConfigs =
            {
                new Grpc.Net.Client.Configuration.MethodConfig
                {
                    Names = { MethodName.Default },
                    RetryPolicy = new RetryPolicy
                    {
                        MaxAttempts = 4,
                        InitialBackoff = TimeSpan.FromMilliseconds(100),
                        MaxBackoff = TimeSpan.FromSeconds(1),
                        BackoffMultiplier = 2,
                        RetryableStatusCodes = { Grpc.Core.StatusCode.Unavailable, Grpc.Core.StatusCode.ResourceExhausted}
                    }
                }
            }
        },
        MaxReceiveMessageSize = 1000*1024*1024,
        MaxSendMessageSize = 1000*1024*1024
    };
}
