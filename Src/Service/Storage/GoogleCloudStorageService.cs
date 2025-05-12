
using System;
using System.IO;
using System.Numerics;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Gateway.Interfaces.Infs;
using Google.Cloud.Storage.V1;

namespace Gateway.Services.Storage;

public class VectorInfo
{
    public string Hash { get; set; }
    public string StorageGuid { get; set; }
    public int Size { get; set; }
    public DateTime CreatedAt { get; set; }
}

public class GcsSqlStorageService : INetworkFileStorageService
{
    private readonly StorageClient _storageClient;
    private readonly string _bucketName;

    public GcsSqlStorageService(string bucketName)
    {
        _storageClient = StorageClient.Create();
        _bucketName = bucketName;
    }

    public async Task StoreVector(string bucket_Id, M_Data data)
    {
        await StoreChunkAsync(data.vector, data.chunk, bucket_Id);
    }

    public static string GenerateChunkKey(float[] vector)
    {
        using var sha256 = SHA256.Create();
        // Convert the float array into bytes
        var byteArray = new byte[vector.Length * sizeof(float)];
        Buffer.BlockCopy(vector, 0, byteArray, 0, byteArray.Length);

        var hashBytes = sha256.ComputeHash(byteArray);

        // Convert to a readable hex string
        var sb = new StringBuilder();
        foreach (var b in hashBytes)
        {
            sb.Append(b.ToString("x2")); // two-digit hex
        }
        return sb.ToString();
    }

    public async Task<bool> StoreChunkAsync(float[] hash, byte[] data, string bucketID)
    {
        try
        {
            string objectName = $"chunks/{GenerateChunkKey(hash)}";

            // Check if chunk already exists in GCS
            var existingObjects = _storageClient.ListObjects(_bucketName, objectName);
            foreach (var obj in existingObjects)
            {
                if (obj.Name == objectName)
                {
                    Console.WriteLine($"Chunk {hash} already exists in GCS.");
                    return false; // No need to store again
                }
            }

            // Upload chunk
            using var memoryStream = new MemoryStream(data);
            await _storageClient.UploadObjectAsync(_bucketName, objectName, null, memoryStream);
            // Console.WriteLine($"Uploaded chunk {hash} to GCS.");
            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Error] StoreChunkAsync failed: {ex.Message}");
            return false;
        }
    }
}
