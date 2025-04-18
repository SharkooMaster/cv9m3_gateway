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
using Agent.Services.Grpc;
using Gateway.Services.Agneta;
using Gateway.Modules.Agneta;
using System.Buffers;
using Gateway.Services.PushOver;
using Gateway.Modules.Pushover;
using Gateway.Utils.Globals;
using Gateway.Modules;
using Gateway.Services.Clms;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddLogging( logging => 
{
    logging.AddConsole();
    logging.SetMinimumLevel(LogLevel.Debug);
});

builder.Services.AddGrpc(options => {
    options.MaxReceiveMessageSize = 1000 * 1024 * 1024;
    options.MaxSendMessageSize = 1000 * 1024 * 1024;
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
await agnetaClientService.ConnectAsync();
AgnetaHandler.SetInstance(agnetaClientService);

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

//app.UseHttpsRedirection();
app.UseRouting();

app.MapGrpcService<GreeterService>();
app.MapGrpcService<BatchSearchService>();
app.MapGrpcService<SearchAllService>();

app.MapGet("/", () =>
{
    return "Hello world";
});

PushoverHandler.PushNotification($"Gateway:{Globals.GATEWAY_ID}: Running");
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
}
