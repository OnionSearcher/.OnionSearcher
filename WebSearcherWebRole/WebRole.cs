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
            Trace.TraceInformation("WebRole.RunAsync : Start");
            try
            {
                RotManager.TryKillTorIfRequired();
                using (RotManager rot = new RotManager(0))
                {
                    await rot.WaitStartAsync(cancellationToken);
                    // main loop
                    DateTime end = DateTime.Now.Add(Settings.Default.TimeBeforeRecycle);
                    while (!cancellationToken.IsCancellationRequested && (DateTime.Now < end) && rot.IsProcessOk())
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

        public override void OnStop()
        {
            RotManager.TryKillTorIfRequired();

            base.OnStop();
        }

    }
}
