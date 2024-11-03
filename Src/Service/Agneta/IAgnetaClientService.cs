namespace Gateway.Services.Agneta;

public interface IAgnetaClientService
{
    Task ConnectAsync();
    Task SendMessageAsync(string message);
    Task<string> RecieveMessageAsync();
}