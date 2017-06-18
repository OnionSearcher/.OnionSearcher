using Microsoft.WindowsAzure.ServiceRuntime;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using WebSearcherCommon;

namespace WebSearcherWorkerRole
{
    public class WorkerRole : RoleEntryPoint, IDisposable
    {

        private CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly DateTime startUp = DateTime.Now;

        public override bool OnStart()
        {
            bool result = base.OnStart();

            Trace.TraceInformation("WorkerRole is starting");

            // Set the maximum number of concurrent connections
            ServicePointManager.DefaultConnectionLimit = 512;

            return result && TorManager.Start();
        }

        public override void Run()
        {
            Trace.TraceInformation("WorkerRole is running");

            try
            {
                this.RunAsync(this.cancellationTokenSource.Token).Wait(this.cancellationTokenSource.Token);
            }
            catch (TaskCanceledException) { }
            catch (Exception ex)
            {
                Trace.TraceError("WorkerRole RunAsync Exception : " + ex.GetBaseException().ToString());
            }
        }

        private List<Task> taskPool;

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            try
            {
                await TorManager.WaitStartedAsync(cancellationToken);

                taskPool = new List<Task>(Settings.Default.NbCrawlersPerInstance);
                
                for (int i = 0; i < taskPool.Capacity; i++)
                    taskPool.Add(null);

                while (!cancellationToken.IsCancellationRequested && (startUp.Add(Settings.Default.TimeBeforeRecycle) > DateTime.Now))
                {
                    // tor check
                    if (!TorManager.IsProcessOk())
                    {
                        Trace.TraceWarning("WorkerRole : Tor KO, restart it");
                        disposeSubTask(); // stop sending request
                        TorManager.Stop();
                        await Task.Delay(1000, cancellationToken);
                        if (!cancellationToken.IsCancellationRequested)
                        {
                            TorManager.Start();
                            await TorManager.WaitStartedAsync(cancellationToken);
                        }
                    }

                    // task manager
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
                            taskPool[i] = Task.Run(() => { CrawlerManager.RunAsync(cancellationToken).Wait(cancellationToken); }, cancellationToken);
                        }
                    }
                    
                    await Task.Delay(60000, cancellationToken);
                }
                Trace.TraceInformation("WorkerRole will wait for a restart");
            }
            catch (TaskCanceledException) { }
            catch (Exception ex)
            {
                Trace.TraceError("WorkerRole.RunAsync Exception : " + ex.GetBaseException().ToString());
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
            }
        }

        private void disposeSubTask()
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
                    try
                    {
                        taskPool[i].Dispose();
                    }
                    catch (InvalidOperationException) { }
                    taskPool[i] = null;
                }
            }
        }

        public override void OnStop()
        {
            Trace.TraceInformation("WorkerRole is stopping");

            this.cancellationTokenSource.Cancel();

            TorManager.Stop();

            // task manager
            if (taskPool != null)
            {
                disposeSubTask();
                taskPool = null;
            }

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
                    // task manager
                    if (taskPool != null)
                    {
                        disposeSubTask();
                        taskPool = null;
                    }
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
