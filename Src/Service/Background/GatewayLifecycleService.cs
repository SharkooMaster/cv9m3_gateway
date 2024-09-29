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
    private readonly IEtcdClientService _etcdClientService;

    public GatewayLifeCycleService(IEtcdClientService etcdClientService)
    {
        Console.WriteLine("INFO::GatewayLifecycleService: Initiating GatewayLifeCycleService");
        _etcdClientService = etcdClientService;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("INFO::GatewayLifecycleService: Starting lifecycle.");

        Console.WriteLine("INFO::GatewayLifecycleService: Starting watch (agents).");
        await _etcdClientService.WatchKey("/agents/", GatewaysWatchTrigger);
        Console.WriteLine("INFO::GatewayLifecycleService: Watching (agents).");
    }

    private void GatewaysWatchTrigger(WatchResponse response)
    {
        if (response.Events.Count == 0)
        {
            Console.WriteLine(response);
        }
        else
        {
            Console.WriteLine($"{response.Events[0].Kv.Key.ToStringUtf8()}:{response.Events[0].Kv.Value.ToStringUtf8()}");
            string _key = response.Events[0].Kv.Key.ToStringUtf8();
            string _val = response.Events[0].Kv.Value.ToStringUtf8();

            if(Globals.AGENTS.ContainsKey(_key))
            {
                Globals.AGENTS.Remove(_key);
            }
            else if(_val != "" && _val != null)
            {
                Globals.AGENTS.Add(_key, JsonConvert.DeserializeObject<M_Agent>(_val) );
            }

            // LOG THE AGENTS LIST FOR DEBUGGING //
            Console.WriteLine("---------------------------");
            foreach(var item in Globals.AGENTS)
            {
                Console.WriteLine($"{item.Key}");
                //Console.WriteLine($"{item.Value.GetID()}");
            }
            Console.WriteLine("---------------------------");
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}