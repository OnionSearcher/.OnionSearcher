using System;
using System.Collections.Generic;
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
        protected readonly DateTime startUp = DateTime.Now;

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections
            ServicePointManager.DefaultConnectionLimit = 1024;

            bool result = base.OnStart(); // will init azure trace

            Trace.TraceInformation("CommonRole is starting");

            return result;
        }
        
        public override void Run()
        {
            Trace.TraceInformation("CommonRole is running");
            
            try
            {
                this.RunAsync(this.cancellationTokenSource.Token).Wait(this.cancellationTokenSource.Token);
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

        protected async Task TorReStarterAsync(CancellationToken cancellationToken)
        {
            if (!TorManager.IsProcessOk())
            {
                Trace.TraceWarning("CommonRole is (re)starting Tor");
                DisposeSubTask(); // stop sending request
                TorManager.Stop();
                await Task.Delay(1000, cancellationToken);
                if (!cancellationToken.IsCancellationRequested)
                {
                    await TorManager.StartAsync(cancellationToken);
                }
            }
        }


        private List<Task> taskPool;
        
        protected void CrawlerReStarterAsync(CancellationToken cancellationToken)
        {
            if (taskPool == null)
            {
#if DEBUG
                taskPool = new List<Task>(Settings.Default.NbCrawlersPerInstance/2);
#else
                taskPool = new List<Task>(Settings.Default.NbCrawlersPerInstance);
#endif
                for (int i = 0; i < taskPool.Capacity; i++)
                    taskPool.Add(null);
            }
            for (int i = 0; i < taskPool.Count; i++)
            {
                Task task = taskPool[i];
                if (task != null && (task.IsCanceled || task.IsCompleted || task.IsFaulted))
                {
                    task.Dispose();
                    taskPool[i] = null;
                }
                if (taskPool[i] == null && !cancellationToken.IsCancellationRequested)
                {
                    taskPool[i] = Task.Run(() =>
                    {
                        try
                        {
                            CrawlerManager.RunAsync(cancellationToken).Wait(cancellationToken);
                        }
                        catch (OperationCanceledException) { }
                        catch (Exception ex)
                        {
                            Trace.TraceError("CrawlerManager.RunAsync Exception : " + ex.GetBaseException().ToString());
#if DEBUG
                            if (Debugger.IsAttached) { Debugger.Break(); }
#endif
                        }
                        Trace.TraceInformation("CrawlerManager.RunAsync[" + i.ToString() + "] finish");
                    }, cancellationToken);
                }
            }
        }

        virtual protected async Task RunAsync(CancellationToken cancellationToken)
        {
            await Task.Delay(100, cancellationToken); // pour évtier le warning
            throw new NotImplementedException("protected async Task RunAsync");
        }

        public override void OnStop()
        {
            Trace.TraceInformation("WorkerRole is stopping");

            this.cancellationTokenSource.Cancel();
            TorManager.Stop();
            DisposeSubTask();

            base.OnStop();
        }

        #region IDisposable Support
        private void DisposeSubTask()
        {
            if (taskPool != null)
            {
                for (int i = 0; i < taskPool.Count; i++)
                {
                    if (taskPool[i] != null)
                    {
                        if (!taskPool[i].IsCompleted || !taskPool[i].IsCanceled || !taskPool[i].IsFaulted)
                            try
                            {
                                taskPool[i].Wait(100);
                            }
                            catch (OperationCanceledException) { }
                            catch (AggregateException) { }
                        try
                        {
                            taskPool[i].Dispose();
                        }
                        catch (InvalidOperationException) { }
                        taskPool[i] = null;
                    }
                }
                taskPool = null;
            }
        }

        private bool disposedValue = false;
        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    DisposeSubTask();
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
