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

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddGrpc();

ConfigureServices(builder.Services);

// Configure Kestrel to allow HTTP/2 without TLS
builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenLocalhost(5000, o => o.Protocols = HttpProtocols.Http2);
    options.ListenLocalhost(5001, o => o.Protocols = HttpProtocols.Http1);
});

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Background services
// builder.Services.AddHostedService<GatewayLifeCycleService>();
// builder.Services.AddHostedService<GatewayRuntimeService>();

var app = builder.Build();

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

app.MapGet("/", () =>
{
    return "Hello world";
});

app.Run();

void ConfigureServices(IServiceCollection services)
{
    services.AddSingleton<AgnetaClientService>(new AgnetaClientService("https://agneta-loadbalancer.default.svc.cluster.local/log/ws"));
    //services.AddSingleton<AgnetaClientService>(new AgnetaClientService("wss://localhost:8080/log/ws"));
    services.AddSingleton<QueryAgentService>();
    services.AddSingleton<StoreBucketRowAgentService>();
}
