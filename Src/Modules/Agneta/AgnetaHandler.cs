using Gateway.Services.Agneta;
using Gateway.Utils.Globals;
using Newtonsoft.Json;

namespace Gateway.Modules.Agneta;

public class LogMessage
{
    [JsonProperty("client_key")]
    public string ClientKey { get; set; }

    [JsonProperty("client_type")]
    public string ClientType { get; set; }

    [JsonProperty("log_level")]
    public int LogLevel { get; set; }

    [JsonProperty("log_message")]
    public string LogMessageText { get; set; }
}


public static class AgnetaHandler
{
    private static AgnetaClientService acs { get; set; }

    public static async Task Log(int _level, string _message)
    {
        if(acs != null)
        {
            LogMessage _log = new LogMessage();
            _log.ClientKey = Globals.ETCD_ID;
            _log.ClientType = "Gateway";
            _log.LogLevel = _level;
            _log.LogMessageText = _message;

            await acs.SendMessageAsync(JsonConvert.SerializeObject(_log));
        }
        else
        {
            Console.WriteLine("ERROR::AgnetaHandler.Log: No service running");
        }
    }
}