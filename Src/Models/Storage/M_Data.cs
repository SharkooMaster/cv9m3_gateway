using System.Text.Json;
using System.Text.Json.Serialization;
using Gateway.Utils.Globals;

public class M_Data
{
    public ulong id { get; set; }
    public float[] vector { get; set; }
    public byte[] chunk { get; set; }

    public string ToJson()
    {
        return JsonSerializer.Serialize(this);
    }

    public static M_Data FromJson(string _json)
    {
        return JsonSerializer.Deserialize<M_Data>(_json);
    }
}
