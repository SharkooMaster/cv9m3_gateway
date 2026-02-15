using System.Collections.Concurrent;
using Gateway.Utils.Globals;
using Grpc.Core;
using Grpc.Net.Client;

namespace Gateway.Services.Grpc;

/// <summary>
/// Service for querying the DHT to find which agent is responsible for a given bucket.
/// Includes caching to avoid repeated DHT queries for the same bucket.
/// </summary>
public class FindPeerResponsibleService
{
    private readonly ILogger<FindPeerResponsibleService>? _logger;
    private readonly ConcurrentDictionary<string, string> _agentCache = new(); // Cache: bitstring -> agent IP
    private readonly TimeSpan _cacheExpiry = TimeSpan.FromMinutes(5);
    private readonly ConcurrentDictionary<string, DateTime> _cacheTimestamps = new();

    public FindPeerResponsibleService(ILogger<FindPeerResponsibleService>? logger = null)
    {
        _logger = logger;
    }

    /// <summary>
    /// Finds the agent responsible for a given bucket (bitstring) using the DHT.
    /// Uses caching to avoid repeated queries for the same bucket.
    /// </summary>
    /// <param name="bitString">The bucket bitstring to find the responsible agent for</param>
    /// <param name="bootstrapAgent">The bootstrap agent IP to start the DHT query (default: agent-1)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The IP address of the agent responsible for this bucket</returns>
    public async Task<string> FindResponsibleAgentAsync(
        string bitString, 
        string? bootstrapAgent = null, 
        CancellationToken cancellationToken = default)
    {
        // Check cache first
        if (_agentCache.TryGetValue(bitString, out var cachedAgent))
        {
            if (_cacheTimestamps.TryGetValue(bitString, out var timestamp) && 
                DateTime.UtcNow - timestamp < _cacheExpiry)
            {
                _logger?.LogDebug($"DHT cache HIT: bucket {bitString} -> agent {cachedAgent}");
                return cachedAgent;
            }
            // Cache expired, remove it
            _agentCache.TryRemove(bitString, out _);
            _cacheTimestamps.TryRemove(bitString, out _);
        }

        try
        {
            // Convert bitstring to ulong for DHT lookup
            ulong bucketKey = BitStringToUlong(bitString);
            
            var bootstrap = bootstrapAgent ?? Globals.AgentsLoadbalancer;
            var client = GrpcChannelFactory.GetClient(
                target: bootstrap,
                ctor: ch => new FindPeerResponsible.FindPeerResponsibleClient(ch),
                roundRobin: false,
                port: 5000
            );

            var request = new QueryReq { Val = bucketKey };
            var deadline = DateTime.UtcNow.AddSeconds(10); // 10 second timeout for DHT query
            
            var response = await client.FindAsync(request, deadline: deadline, cancellationToken: cancellationToken);
            
            var responsibleAgent = response.Res;
            
            // Cache the result
            _agentCache.TryAdd(bitString, responsibleAgent);
            _cacheTimestamps.TryAdd(bitString, DateTime.UtcNow);
            
            _logger?.LogDebug($"DHT query: bucket {bitString} (key: {bucketKey}) -> agent {responsibleAgent}");
            return responsibleAgent;
        }
        catch (RpcException ex)
        {
            _logger?.LogWarning(ex, $"DHT query failed for bucket {bitString}, falling back to bootstrap agent");
            // Fall back to bootstrap agent if DHT query fails
            return bootstrapAgent ?? Globals.AgentsLoadbalancer;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, $"Error finding responsible agent for bucket {bitString}");
            // Fall back to bootstrap agent on any error
            return bootstrapAgent ?? Globals.AgentsLoadbalancer;
        }
    }

    /// <summary>
    /// Converts a bitstring to ulong for DHT lookup.
    /// </summary>
    public static ulong BitStringToUlong(string bitString)
    {
        try
        {
            return Convert.ToUInt64(bitString, 2);
        }
        catch
        {
            // If conversion fails, use hash of string as fallback
            return (ulong)Math.Abs(bitString.GetHashCode());
        }
    }

    /// <summary>
    /// Clears the agent cache (useful for testing or when DHT topology changes).
    /// </summary>
    public void ClearCache()
    {
        _agentCache.Clear();
        _cacheTimestamps.Clear();
        _logger?.LogInformation("DHT agent cache cleared");
    }
}

