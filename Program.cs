using System.Net;
using System.Net.Security;
using Gateway.Services;
using Gateway.Services.Etcd;
using dotnet_etcd;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Logging;

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
builder.Services.AddHostedService<GatewayLifeCycleService>();
//builder.Services.AddHostedService<GatewayRuntimeService>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

//app.UseHttpsRedirection();
app.UseRouting();

app.MapGrpcService<GreeterService>();
app.MapGet("/", () =>
{
    return "Hello world";
});

app.Run();

void ConfigureServices(IServiceCollection services)
{
    Console.WriteLine("Initiating iEtcd");

    services.AddSingleton<EtcdClient>(provider => {
        var configuration = provider.GetRequiredService<IConfiguration>();
        var etcdUrl = configuration["Etcd:Url"];
		Console.WriteLine($"etcd_url: {etcdUrl}");

		return new EtcdClient(etcdUrl, configureChannelOptions: (options => {
            options.Credentials = ChannelCredentials.Insecure;
        }));
    });
    services.AddSingleton<IEtcdClientService, EtcdClientService>();
	Console.WriteLine("Connected iEtcd");
}
