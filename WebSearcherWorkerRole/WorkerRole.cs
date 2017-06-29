using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using WebSearcherCommon;

namespace WebSearcherWorkerRole
{
    public class WorkerRole : CommonRole
    {

        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            try
            {
                PerfCounter.Init();

                DateTime end = DateTime.Now.Add(Settings.Default.TimeBeforeRecycle);

                // main loop
                while (!cancellationToken.IsCancellationRequested && (DateTime.Now < end))
                {
                    await TorReStarterAsync(cancellationToken);

                    CrawlerReStarterAsync(cancellationToken);

                    await Task.Delay(60000, cancellationToken);
                }
            }
            catch (OperationCanceledException)
            {
                Trace.TraceWarning("WorkerRole.RunAsync OperationCanceled");
            }
            catch (Exception ex)
            {
                Trace.TraceError("WorkerRole.RunAsync Exception : " + ex.GetBaseException().ToString());
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
            }
            Trace.TraceInformation("WorkerRole.RunAsync : wait for a restart");
        }

    }
}
