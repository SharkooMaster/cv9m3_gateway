using Gateway.Services.Agneta;

namespace Gateway.Modules.Agneta;

public static class AgnetaHandler
{
    private static AgnetaClientService acs { get; set; }

    public static async Task Log(int _level, string _message)
    {
        if(acs != null)
        {
            string _log = "";
            await acs.SendMessageAsync(_log);
        }
        else
        {
            Console.WriteLine("ERROR::AgnetaHandler.Log: No service running");
        }
    }
}