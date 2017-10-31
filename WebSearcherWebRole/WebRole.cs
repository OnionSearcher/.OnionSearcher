using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using WebSearcherCommon;

namespace WebSearcherWebRole
{
    public class WebRole : CommonRole
    {

        // Trace not working on WebRole class itself (but works for the website)
        public override bool OnStart()
        {
            foreach (TraceListener tl in Trace.Listeners)
                if (tl.Name == "AzureDiagnostics") // && tl is DiagnosticMonitorTraceListener
                {
                    tl.Flush(); // force a call on the tracer itself (and this unlock the tracer who works then !)
                    break;
                }

            return base.OnStart();
        }

        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            Trace.TraceInformation("WebRole.RunAsync : Start");
            RotManager.TryKillTorIfRequired();
            try
            {
                using (RotManager rot0 = new RotManager(0))
                using (RotManager rot1 = new RotManager(1))
                using (RotManager rot2 = new RotManager(2))
                {
                    await rot0.WaitStartAsync(cancellationToken);
                    await rot1.WaitStartAsync(cancellationToken);
                    await rot2.WaitStartAsync(cancellationToken);
                    // main loop
                    while (!cancellationToken.IsCancellationRequested && rot0.IsProcessOk() && rot1.IsProcessOk() && rot2.IsProcessOk())
                    {
                        await Task.Delay(30000, cancellationToken);
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                Trace.TraceError("WebRole.RunAsync Exception : " + ex.GetBaseException().ToString());
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
            }
            Trace.TraceInformation("WebRole.RunAsync : End");
        }
        
    }
}
