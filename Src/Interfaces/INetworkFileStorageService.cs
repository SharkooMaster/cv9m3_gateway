
using System.Numerics;
using Gateway.Models;

namespace Gateway.Interfaces.Infs
{
    public interface INetworkFileStorageService
    {
        public Task StoreVector(string bucket_Id, M_Data data);
        // Store bucket data in files where the name of the file is the 128bit ID.
    }
}