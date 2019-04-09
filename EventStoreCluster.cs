using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace es_cluster_scavenge
{
    class EventStoreCluster
    {
        public static async Task<EventStoreCluster> FromDns(string hostNameOrAddress, int extHttpPort, string user, string pass)
            => new EventStoreCluster(hostNameOrAddress, extHttpPort, user, pass, await Dns.GetHostAddressesAsync(hostNameOrAddress));

        public EventStoreCluster(string hostNameOrAddress, int extHttpPort, string user, string pass, params IPAddress[] nodeEndpoints)
        {
            if (string.IsNullOrWhiteSpace(hostNameOrAddress))
                throw new ArgumentException("host cannot be null, empty or whitespace", nameof(hostNameOrAddress));

            if (nodeEndpoints == null || nodeEndpoints.Length < 1)
                throw new ArgumentException(nameof(nodeEndpoints));

            ClusterDns = hostNameOrAddress;

            var nodeExtHttpEndpoints = nodeEndpoints.Select(e => new IPEndPoint(e, extHttpPort));

            ClusterNodes = nodeExtHttpEndpoints
                .Select(e => new EventStoreNode(e, user, pass))
                .ToArray();
        }

        public string ClusterDns { get; }
        public IReadOnlyList<EventStoreNode> ClusterNodes { get; }

        public async Task StopAllExistingScavenges()
        {
            // get all tasks - start at chunk 0 for all...
            var tasks = ClusterNodes
                .Select(cn => cn.StopAllScavenges());

            await Task.WhenAll(tasks);
        }

        public async Task RunFullScavengeAsync(int threads, CancellationToken ct)
        {
            // get all tasks - start at chunk 0 for all...
            var tasks = ClusterNodes
                .Select(cn => cn.RunScavengeAsync(0, threads, ct));

            await Task.WhenAll(tasks);
        }

        public async Task RunScavengeAsync(TimeSpan retentionDuration, int threads, CancellationToken ct)
        {
            // get all tasks
            var tasks = ClusterNodes
                .Select(cn => cn.RunScavengeAsync(retentionDuration, threads, ct));

            await Task.WhenAll(tasks);
        }
    }
}
