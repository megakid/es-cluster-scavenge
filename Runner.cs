using System;
using System.Net;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;

namespace es_cluster_scavenge
{
    class Runner
    {
        private CommandLineOptions _options;
        private IConsole _console;
        private ConsoleReporter _reporter;

        public Runner(CommandLineOptions options, IConsole console)
        {
            _options = options;
            _console = console;
            _reporter = new ConsoleReporter(console)
            {
                IsQuiet = options.Quiet,
                IsVerbose = options.Verbose,
            };
        }

        private async Task Start()
        {
            var node = new EventStoreNode(new IPEndPoint(
                IPAddress.Loopback, 2113), "admin", "changeit");

            var scavenge = await node.FindLastScavenge();

            _reporter.Error(scavenge.Result);
        }

        private async Task Stop()
        {
        }

        public async Task<int> RunAsync()
        {
            var cts = new CancellationTokenSource();

            _console.CancelKeyPress += (_, __) => cts.Cancel();

            try
            {
                await Start();

                await cts.Token.ThrowWhenCanceled();
            }
            catch (Exception) when (cts.IsCancellationRequested)
            {
                // ignore
                return 0;
            }
            finally
            {
                // either way, gracefully shutdown
                await Stop();
            }

            return -1;
        }

    }
}
