using Gateway.Services.Etcd;
using Gateway.Utils.Globals;
using dotnet_etcd;
using Etcdserverpb;
using Google.Protobuf;
using Gateway.Models;
using System.Text.Json.Serialization;
using Newtonsoft.Json;

namespace Gateway.Services;

public class GatewayLifeCycleService : IHostedService
{

    public GatewayLifeCycleService()
    {
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
    }

    private void GatewaysWatchTrigger(WatchResponse response)
    {
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}