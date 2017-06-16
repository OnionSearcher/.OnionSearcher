using Microsoft.WindowsAzure.ServiceRuntime;
using System;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using WebSearcherCommon;

namespace WebSearcherWebRole
{
    public class WebRole : RoleEntryPoint, IDisposable
    {
        private CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly DateTime startUp = DateTime.Now;

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections
            ServicePointManager.DefaultConnectionLimit = 1024;

            // TOFIX : can t manage the Server header Microsoft-HTTPAPI/2.0" from http response ...
            //string errRegistry = null;
            //try
            //{
            //    Registry.LocalMachine.OpenSubKey(@"SYSTEM\CurrentControlSet\Services\HTTP\Parameters")..SetValue("DisbableServerHeader", 1, RegistryValueKind.DWord); // before onstart http service
            //}
            //catch (Exception ex)
            //{
            //    errRegistry = ex.GetBaseException().ToString();
#if DEBUG
            //    if (Debugger.IsAttached) { Debugger.Break(); }
#endif
            //}

            bool result = base.OnStart(); // will init azure trace

            Trace.TraceInformation("WebRole is starting");

            return result && TorManager.Start();
        }

        public override void Run()
        {
            Trace.TraceInformation("WebRole is running");

            try
            {
                this.RunAsync(this.cancellationTokenSource.Token).Wait();
            }
            catch (TaskCanceledException) { }
            catch (Exception ex)
            {
                Trace.TraceError("WebRole Run Exception : " + ex.GetBaseException().ToString());
            }
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested && (startUp.Add(Settings.Default.TimeBeforeRecycle) > DateTime.Now))
                {
                    // tor check
                    if (!TorManager.IsProcessOk())
                    {
                        Trace.TraceInformation("Tor KO, restart it");
                        TorManager.Stop();
                        await Task.Delay(1000, cancellationToken);
                        if (!cancellationToken.IsCancellationRequested)
                        {
                            TorManager.Start();
                            await TorManager.WaitStartedAsync(cancellationToken);
                            TorManager.TraceHostname();
                        }
                    }
                    await Task.Delay(20000, cancellationToken);
                }
            }
            catch (TaskCanceledException) { }
            catch (Exception ex)
            {
                Trace.TraceError("WebRole RunAsync Exception : " + ex.GetBaseException().ToString());
            }
        }

        public override void OnStop()
        {
            Trace.TraceInformation("WebRole is stopping");

            this.cancellationTokenSource.Cancel();

            TorManager.Stop();

            base.OnStop();
        }

        #region IDisposable Support
        private bool disposedValue = false;
        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    cancellationTokenSource.Dispose();
                    cancellationTokenSource = null;
                }
                disposedValue = true;
            }
        }
        public void Dispose()
        {
            Dispose(true);
        }
        #endregion

    }
}
