
namespace Gateway.Models;

public class M_Agent
{
    /*
    {
        "id":"0a1e3561-6078-4562-94c0-a2f59d1d580f",
        "name":"agent",
        "host":"10.244.0.18",
        "port":"5000",
        "url":"http://10.244.0.18:5000",
        "healthCheck":"http://10.244.0.18:5000/health",
        "version":"v1.0.0",
        "metadata": {
            "environment":"dev"
        }
    }
    */

    public string id { get; set; }
    public string name { get; set; }
    public string host { get; set; }
    public string port { get; set; }
    public string url { get; set; }
    public string healthCheck { get; set; }
    public string version { get; set; }
    public object metadata { get; set; }

    public string GetID()
    {
        return id;
    }
}