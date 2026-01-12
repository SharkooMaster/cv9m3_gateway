
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
    private StorageClient? _storageClient;
    private readonly string _bucketName;
    private readonly object _storageClientLock = new object();

    public GcsSqlStorageService(string bucketName)
    {
        _bucketName = bucketName;
        // Don't initialize StorageClient here - initialize lazily when needed
    }

    private StorageClient? GetStorageClient()
    {
        if (_storageClient != null)
            return _storageClient;

        lock (_storageClientLock)
        {
            if (_storageClient != null)
                return _storageClient;

            try
            {
                _storageClient = StorageClient.Create();
                return _storageClient;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Warning] Failed to initialize GCS client: {ex.Message}. GCS operations will be disabled.");
                return null;
            }
        }
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
        var client = GetStorageClient();
        if (client == null)
        {
            Console.WriteLine("[Warning] GCS client not available, skipping chunk storage");
            return false;
        }

        try
        {
            string objectName = $"chunks/{GenerateChunkKey(hash)}";

            // Check if chunk already exists in GCS
            var existingObjects = client.ListObjects(_bucketName, objectName);
            foreach (var obj in existingObjects)
            {
                if (obj.Name == objectName)
                {
                    Console.WriteLine($"Chunk {objectName} already exists in GCS.");
                    return false; // No need to store again
                }
            }

            // Upload chunk
            using var memoryStream = new MemoryStream(data);
            await client.UploadObjectAsync(_bucketName, objectName, null, memoryStream);
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
