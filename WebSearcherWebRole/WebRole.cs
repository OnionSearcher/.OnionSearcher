using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using WebSearcherCommon;

namespace WebSearcherWebRole
{
    public class WebRole : CommonRole
    {

        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            try
            {
                DateTime end = DateTime.Now.Add(Settings.Default.TimeBeforeRecycle);

                // main loop
                while (!cancellationToken.IsCancellationRequested && (DateTime.Now < end))
                {
                    await TorReStarterAsync(cancellationToken);

                    await Task.Delay(20000, cancellationToken);
                }
            }
            catch (OperationCanceledException)
            {
                Trace.TraceWarning("WebRole.RunAsync OperationCanceled");
            }
            catch (Exception ex)
            {
                Trace.TraceError("WebRole.RunAsync Exception : " + ex.GetBaseException().ToString());
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
            }
            Trace.TraceInformation("WebRole.RunAsync : wait for a restart");
        }

    }
}
