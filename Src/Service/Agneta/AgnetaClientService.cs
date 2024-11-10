using System.Net.WebSockets;
using System.Text;
using Grpc.Core;
using Newtonsoft.Json;

namespace Gateway.Services.Agneta;

public class CloseSocketMessage
{
    public string cmd {get;set;}
}

public class AgnetaClientService : IAgnetaClientService
{
    private ClientWebSocket _client;
    private readonly Uri _uri;

    public AgnetaClientService(string uri)
    {
        _uri = new Uri(uri);
        _client = new ClientWebSocket();
    }
    
    public async Task ConnectAsync()
    {
        _client.Options.RemoteCertificateValidationCallback = (sender, certificate, chain, sslPolicyErrors) => true;
        if(_client.State != WebSocketState.Open)
        {
            await _client.ConnectAsync(_uri, CancellationToken.None);
        }
    }

    public async Task SendMessageAsync(string message)
    {
        if(_client.State != WebSocketState.Open)
        {
            await ConnectAsync();
        }

        var buffer = Encoding.UTF8.GetBytes(message);
        await _client.SendAsync(new ArraySegment<byte>(buffer), WebSocketMessageType.Text, true, CancellationToken.None);
        Console.WriteLine($"Log sent to Agneta: {message}");
    }

    public async Task<string> RecieveMessageAsync()
    {
        var buffer = new byte[1024];
        var result = await _client.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
        return Encoding.UTF8.GetString(buffer, 0, result.Count);
    }

    public async Task SendCloseAsync()
    {
        CloseSocketMessage csm = new CloseSocketMessage(){ cmd = "unsubscribe_logs" };
        await SendMessageAsync(JsonConvert.SerializeObject(csm));
    }
}
