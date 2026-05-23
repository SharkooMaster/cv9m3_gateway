using System.Net;
using System.Net.Security;
using Gateway.Services;
using Gateway.Services.Etcd;
using Gateway.Services.Grpc;
using dotnet_etcd;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Logging;
using Gateway.Services.Grpc;
using Gateway.Services.Agneta;
using Gateway.Modules.Agneta;
using System.Buffers;
using Gateway.Services.PushOver;
using Gateway.Modules.Pushover;
using Gateway.Utils.Globals;
using Gateway.Modules;
using Gateway.Services.Clms;
using Gateway.Services.Storage;
using Gateway.Utils;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

var builder = WebApplication.CreateBuilder(args);

/* builder.Services.AddLogging( logging => 
{
    logging.AddConsole();
    logging.SetMinimumLevel(LogLevel.Debug);
}); */

builder.Services.AddGrpc(options => {
    options.MaxReceiveMessageSize = 1000 * 1024 * 1024;
    options.MaxSendMessageSize = 1000 * 1024 * 1024;
});

builder.Services.AddOpenTelemetry()
    .WithTracing(tracing =>
    {
        tracing
            .SetResourceBuilder(Observability.CreateResourceBuilder())
            .AddSource("CrossV9.Gateway")
            .AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation()
            .AddOtlpExporter(otlp =>
            {
                otlp.Endpoint = new Uri(Observability.GetOtlpEndpoint());
                otlp.Protocol = OpenTelemetry.Exporter.OtlpExportProtocol.Grpc;
            });
    })
    .WithMetrics(metrics =>
    {
        metrics
            .SetResourceBuilder(Observability.CreateResourceBuilder())
            .AddMeter("CrossV9.Gateway")
            .AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation()
            .AddRuntimeInstrumentation()
            .AddPrometheusExporter();
    });

ConfigureServices(builder.Services);

// Configure Kestrel to allow HTTP/2 without TLS
builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(5000, o => o.Protocols = HttpProtocols.Http2);
    options.ListenAnyIP(5001, o => o.Protocols = HttpProtocols.Http1);
    options.Limits.MaxRequestBodySize = null;
    
    options.Limits.KeepAliveTimeout = TimeSpan.FromMinutes(10);
    // options.Limits.Http2.KeepAlivePingDelay = TimeSpan.FromSeconds(30);
    // options.Limits.Http2.KeepAlivePingTimeout = TimeSpan.FromSeconds(60);
});

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Background services
// builder.Services.AddHostedService<GatewayLifeCycleService>();
// builder.Services.AddHostedService<GatewayRuntimeService>();

// EtcdMembershipWatcher subscribes to /agents/ in etcd and pushes the
// resulting ConsistentHashRing into RingState. Keeping this in sync with
// cross's watcher is what guarantees that gateway and cross pick the
// same R replicas for a given key — without it, a write batch could be
// fanned out to one set of agents by gateway while cross expects a
// different set, breaking write quorum.
builder.Services.AddHostedService<Gateway.Utilities.EtcdMembershipWatcher>();

// RebalanceCoordinator watches RingState for topology changes and
// orchestrates vnode handoffs (BeginVnodeAdoption RPCs to newly-joined
// agents). Disabled by default — set REBALANCE_COORDINATOR_ENABLED=true
// to opt in. Intended for exactly one gateway pod at a time; we keep
// this lightweight enough that running multiple is safe (the
// determinism in HandleTopologyChange means duplicate dispatches just
// re-confirm the same handoff; the dst agent's adoption_id de-dup
// prevents double-streaming).
builder.Services.AddHostedService<Gateway.Services.RebalanceCoordinator>();

var app = builder.Build();

var clmsClientService = app.Services.GetRequiredService<ClmsClientService>();
ClmsHandler.SetClmsInstance(clmsClientService);

var pushoverClientService = app.Services.GetRequiredService<PushoverClientService>();
PushoverHandler.SetInstance(pushoverClientService);

var agnetaClientService = app.Services.GetRequiredService<AgnetaClientService>();
// await agnetaClientService.ConnectAsync();
// AgnetaHandler.SetInstance(agnetaClientService);

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

//app.UseHttpsRedirection();
app.UseRouting();
app.MapPrometheusScrapingEndpoint("/metrics");

// Tiny pod-self-reporting endpoint pulled by the control-center every ~30 s.
// Allocation-bounded; never touches request routing.
Gateway.Utils.RuntimeStatsEndpoint.Map(app, "gateway");

app.MapGrpcService<GreeterService>();
app.MapGrpcService<BatchSearchService>();
app.MapGrpcService<SearchAllService>();

var networkFileSystemService = app.Services.GetRequiredService<GcsSqlStorageService>(); // Drop in replacement for nfs with gcp
NetworkFileStorageHandler.SetInstance(networkFileSystemService);

app.MapGet("/", () =>
{
    return "Hello world";
});

// ── Drain status endpoint (HPA-safe scale-down) ─────────────────────
// Polled by an agent's preStop hook (POST /admin/retire on the agent
// kicks off the drain and then polls this endpoint) to decide when
// the pod is safe to terminate.
//
// Returns a snapshot of the RetirementTracker for the named pod:
//   { pod, total, pending, completed, failed, complete, started_utc,
//     last_update_utc, reason }
//
// Semantics:
//   - complete=true means every adoption dispatched against the pod
//     has reached a terminal state (COMPLETED or FAILED).
//   - When the pod isn't tracked at all (e.g. ring was already empty
//     or this gateway never observed the drain), we return
//     complete=true, reason="not tracked" so the agent can proceed
//     instead of blocking forever.
//
// Authentication: none. Cluster-internal only — exposed on the same
// HTTP port as /stats/runtime, which is already cluster-internal.
app.MapGet("/admin/retirement-status", (HttpContext ctx) =>
{
    var pod = ctx.Request.Query["pod"].ToString();
    if (string.IsNullOrEmpty(pod))
    {
        return Results.BadRequest(new { error = "missing pod query parameter" });
    }
    var snapshot = Gateway.Services.RetirementTracker.GetStatus(pod);
    return Results.Json(new
    {
        pod = snapshot.Pod,
        total = snapshot.Total,
        pending = snapshot.Pending,
        completed = snapshot.Completed,
        failed = snapshot.Failed,
        complete = snapshot.Complete,
        started_utc = snapshot.StartedUtc,
        last_update_utc = snapshot.LastUpdateUtc,
        reason = snapshot.Reason
    });
});

PushoverHandler.PushNotification($"Gateway:{Globals.GATEWAY_ID}: Running");

// PRE-WARM CONNECTIONS: Establish gRPC channels to agents at startup
// This eliminates connection setup latency for first requests
if (LocalModeDetector.IsLocalMode())
{
    Console.WriteLine("[Gateway] Pre-warming gRPC connections in local mode...");
    try
    {
        // Pre-warm connection to agent-1
        // Just getting the channel is enough to establish the connection
        var channel = GrpcChannelFactory.GetChannel(Globals.AgentsLoadbalancer, 5000);
        Console.WriteLine("[Gateway] Pre-warmed connection to agent-1");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[Gateway] Warning: Failed to pre-warm connection: {ex.Message}");
    }
}

// ── Global exception handlers to prevent silent crashes ──
AppDomain.CurrentDomain.UnhandledException += (sender, eventArgs) =>
{
    Console.WriteLine($"[GATEWAY UNHANDLED EXCEPTION]: {eventArgs.ExceptionObject}");
};

TaskScheduler.UnobservedTaskException += (sender, e) =>
{
    Console.WriteLine($"[GATEWAY UNOBSERVED TASK EXCEPTION]: {e.Exception}");
    e.SetObserved(); // Prevent process termination
};

app.Run();

await AgnetaHandler.Close();
Console.WriteLine("Connection closed with AgnetaClientService");

void ConfigureServices(IServiceCollection services)
{
    services.AddSingleton<ClmsClientService>(new ClmsClientService());
    services.AddSingleton<AgnetaClientService>(new AgnetaClientService("wss://192.168.50.240:443/log/ws"));
    //services.AddSingleton<AgnetaClientService>(new AgnetaClientService("wss://localhost:8080/log/ws"));
    services.AddSingleton<PushoverClientService>(new PushoverClientService());
    services.AddSingleton<QueryAgentService>();
    services.AddSingleton<StoreBucketRowAgentService>();
    
    services.AddSingleton<GcsSqlStorageService>(
        new GcsSqlStorageService(
            "cross-global-chunks"
        )
    );
}
