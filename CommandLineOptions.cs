using System.ComponentModel.DataAnnotations;
using McMaster.Extensions.CommandLineUtils;

namespace es_cluster_scavenge
{
    [Command(Description = "EventStore scavenge command line tool.")]
    class CommandLineOptions
    {

        [Argument(0, Description = "The DNS host with all the nodes added.\nMust point to all the nodes you want to scavenge.")]
        //[Required]
        public string Host { get; }

        [Option(Description = "An optional External Tcp Port, with a default value of 1113.\nMust be same for all nodes in the cluster.")]
        [Range(1, 65535)]
        public int TcpPort { get; } = 1113;

        [Option(Description = "An optional External Http Port, with a default value of 2113.\nMust be same for all nodes in the cluster.")]
        [Range(1, 65535)]
        public int HttpPort { get; } = 2113;


        [Option(Description = "Show less console output.")]
        public bool Quiet { get; } = false;

        [Option(Description = "Show more console output.")]
        public bool Verbose { get; } = false;
    }
}
