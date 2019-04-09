using System.Threading;
using System.Threading.Tasks;

namespace es_cluster_scavenge
{
    public static class Extensions
    {
        public static async Task ThrowWhenCanceled(this CancellationToken cancellationToken)
        {
            await cancellationToken.WhenCanceled();

            cancellationToken.ThrowIfCancellationRequested();
        }
        public static Task WhenCanceled(this CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            cancellationToken.Register(s => ((TaskCompletionSource<bool>)s).SetResult(true), tcs);
            return tcs.Task;
        }
    }
}
