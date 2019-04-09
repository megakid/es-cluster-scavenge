using System;
using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;

namespace es_cluster_scavenge
{

    class Program
    {
        // Return codes
        private const int ERROR = 2;
        private const int OK = 0;

        public static async Task<int> Main(string[] args)
        {
            DebugHelper.HandleDebugSwitch(ref args);

            try
            {
                var app = new CommandLineApplication<CommandLineOptions>();
                app.Conventions.UseDefaultConventions();
                app.OnExecute(async () =>
                {
                    var server = new Runner(app.Model, PhysicalConsole.Singleton);
                    return await server.RunAsync();
                });
                return app.Execute(args);
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.Error.WriteLine("Unexpected error: " + ex.ToString());
                Console.ResetColor();
                return ERROR;
            }
        }
    }

}
