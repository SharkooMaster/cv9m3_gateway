using Gateway.Services.Etcd;
using Gateway.Utils.Globals;
namespace Gateway.Services;

public class GatewayRuntimeService : BackgroundService
{
    private readonly IEtcdClientService _etcdClientService;

    public GatewayRuntimeService(IEtcdClientService etcdClientService)
    {
        Console.WriteLine("INFO::GatewayRuntimeService: Initiating GatewayRuntimeService");
        _etcdClientService = etcdClientService;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(8), stoppingToken);
            }
            catch (TaskCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
        }

    }

    public override async Task StopAsync(CancellationToken stoppingToken)
    {
        await base.StopAsync(stoppingToken);
    }
}