using System;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.ServiceRuntime;

namespace WebSearcherCommon
{
    public class CommonRole : RoleEntryPoint, IDisposable
    {

        private CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

        private void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            if (e.ExceptionObject is Exception ex)
            {
                Trace.TraceError("CommonRole.CurrentDomain_UnhandledException : " + ex.GetBaseException().Message, ex);
                if(ex is OutOfMemoryException)  // may be raised by System.Net.WebClient.DownloadBitsState.RetrieveBytes
                {
                    cancellationTokenSource.Cancel();
                }
            }
            else
            {
                Trace.TraceError("CommonRole.CurrentDomain_UnhandledException : NULL");
            }
#if DEBUG
            if (Debugger.IsAttached) { Debugger.Break(); }
#endif
        }

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections
            ServicePointManager.DefaultConnectionLimit = 1024;

            bool result = base.OnStart(); // will init azure trace

            Trace.TraceInformation("CommonRole is starting");
            AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;

            return result;
        }
        
        public override void Run()
        {
            Trace.TraceInformation("CommonRole is running");
            
            try
            {
                RunAsync(cancellationTokenSource.Token).Wait(cancellationTokenSource.Token); // RunAsync will be override
            }
            catch (OperationCanceledException)
            {
                Trace.TraceInformation("CommonRole.Run OperationCanceled");
            }
            catch (Exception ex)
            {
                Trace.TraceError("CommonRole.Run Exception : " + ex.GetBaseException().ToString());
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
            }
        }
        
        /// <summary>
        /// must be override
        /// </summary>
        virtual protected async Task RunAsync(CancellationToken cancellationToken)
        {
            await Task.Delay(100, cancellationToken); // just for avoid compil warning of not async
            throw new NotImplementedException();
        }

        public override void OnStop()
        {
            Trace.TraceInformation("CommonRole is stopping");

            cancellationTokenSource.Cancel();

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
