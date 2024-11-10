namespace Gateway.Services.Pushover;

public interface IPushoverClientService
{
    Task PushNotificationAsync(string message);
}