using Gateway.Models;
using Gateway.Services.Grpc;
using Gateway.Utils.Misc;
using Grpc.Net.Client;
using Grpc.Net.Client.Configuration;
using System;
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

    // Similarity threshold (byte-level). Matches your algorithm spec default: 0.60.
    // Override via env var MIN_THRESH if desired.
    public static float MinThresh =
        float.TryParse(Environment.GetEnvironmentVariable("MIN_THRESH"), out var mt) ? mt : 0.60f;
    // K=1 => radius 1 search: original bucket + all single-bit flips
    // For 64-bit bitstrings: 65 buckets total (original + 64 flips)
    // This is the MINIMUM required for proper deduplication
    public static int K = 1;

    // AGENTS_CONFIG
    // Allow running outside Kubernetes/Docker by overriding via env var.
    // Examples:
    // - AGENTS_LOADBALANCER=localhost
    // - AGENTS_LOADBALANCER=127.0.0.1
    // - AGENTS_LOADBALANCER=agent-1 (docker-compose / k8s service)
    public static string AgentsLoadbalancer = Environment.GetEnvironmentVariable("AGENTS_LOADBALANCER") ?? "agent-1";
    public static SearchVectorService svs = new SearchVectorService();
    public static StoreVectorService svec = new StoreVectorService();
    public static FindPeerResponsibleService dhtService = new FindPeerResponsibleService();

    // LOCAL MODE: Reduce retries in local mode (local network is reliable)
    // Note: gRPC requires MaxAttempts > 1, so use 2 for local mode
    private static int GetMaxRetryAttempts()
    {
        return LocalModeDetector.IsLocalMode() ? 2 : 4;
    }

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
                        MaxAttempts = GetMaxRetryAttempts(),
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
