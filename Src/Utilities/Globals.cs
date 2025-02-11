using Gateway.Models;
using Gateway.Utils.Misc;
namespace Gateway.Utils.Globals;

public static class Globals
{
    // ETCD_CONFIG
    //public static string ETCD_ID = "";
    //public static string ETCD_VALUE = "";
    //public static long ETCD_LEASE_ID = -1;

    public static string GATEWAY_ID         = Misc.Misc.GenerateId();
    public static string GATEWAY_VALUE      = "";
    public static long   GATEWAY_LEASE_ID   = -1;

    public static float MinThresh = 0.6f;
    public static int K = 4;

    // AGENTS_CONFIG
    public static string AgentsLoadbalancer = "http://agent-loadbalancer.default.svc.cluster.local:5000";
}
