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
    options.Limits.MaxRequestBodySize = 1024 * 1024 * 1024;
    
    options.Limits.KeepAliveTimeout = TimeSpan.FromMinutes(10);
    options.Limits.Http2.KeepAlivePingDelay = TimeSpan.FromSeconds(30);
    options.Limits.Http2.KeepAlivePingTimeout = TimeSpan.FromSeconds(60);
});

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Background services
// builder.Services.AddHostedService<GatewayLifeCycleService>();
// builder.Services.AddHostedService<GatewayRuntimeService>();

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
