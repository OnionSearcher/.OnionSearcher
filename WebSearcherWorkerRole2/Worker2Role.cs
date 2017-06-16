using Microsoft.WindowsAzure.ServiceRuntime;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using WebSearcherCommon;

namespace WebSearcherWorkerRole2
{
    public class Worker2Role : RoleEntryPoint, IDisposable
    {

        private CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

        private readonly DateTime startUp = DateTime.Now;
        private DateTime lastComputeIndexedPages;
        private DateTime lastUpdateHiddenServicesRank;
        private DateTime lasPagesPurge;
        private DateTime lastUpdatePageRank;
        private DateTime lastHDRootRescan;

        public override bool OnStart()
        {
            bool result = base.OnStart();

            Trace.TraceInformation("Worker2Role is starting");

            Random rnd = new Random();
            lastComputeIndexedPages = DateTime.Now.AddMinutes(-1.0 * (
                rnd.Next((int)Settings.Default.ComputeIndexedPagesTaskDelay.TotalMinutes)
            ));
            lastUpdateHiddenServicesRank = DateTime.Now.AddMinutes(-1.0 * (
                rnd.Next((int)Settings.Default.UpdateHiddenServicesRankDelay.TotalMinutes)
            ));
            lastUpdatePageRank = DateTime.Now.AddMinutes(-1.0 * (
                rnd.Next((int)Settings.Default.UpdatePageRankTaskDelay.TotalMinutes)
            ));
            lasPagesPurge = DateTime.Now.AddMinutes(-1.0 * (
                rnd.Next((int)Settings.Default.PagesPurgeTaskDelay.TotalMinutes)
            ));
            lastHDRootRescan = DateTime.Now.AddMinutes(-1.0 * (
                rnd.Next((int)Settings.Default.HDRootRescanDelay.TotalMinutes)
            ));

#if DEBUG
            // force to run all task
            lastComputeIndexedPages = DateTime.MinValue;
            lastUpdateHiddenServicesRank = DateTime.MinValue;
            lastUpdatePageRank = DateTime.MinValue;
            lasPagesPurge = DateTime.MinValue;
            lastHDRootRescan = DateTime.MinValue;
#endif

            return result;
        }

        public override void Run()
        {
            Trace.TraceInformation("Worker2Role is running");

            try
            {
                this.RunAsync(this.cancellationTokenSource.Token).Wait(this.cancellationTokenSource.Token);
            }
            catch (TaskCanceledException) { }
            catch (Exception ex)
            {
                Trace.TraceError("Worker2Role Run Exception : " + ex.GetBaseException().ToString());
            }
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested && (startUp.Add(Settings.Default.TimeBeforeRecycle) > DateTime.Now))
                {
                    bool stillStuffToDo = false;
                    using (SqlManager sql = new SqlManager()) // the connection won't be open if not used.
                    {
                        // pre UpdatePageRankAsync
                        if (!cancellationToken.IsCancellationRequested && lastComputeIndexedPages.Add(Settings.Default.ComputeIndexedPagesTaskDelay) < DateTime.Now)
                        {
                            //Trace.TraceInformation("Worker2Role calling ComputeIndexedPages"); // verbose
                            await sql.ComputeIndexedPagesAsync(cancellationToken);
                            lastComputeIndexedPages = DateTime.Now;
                        }
                        if (!cancellationToken.IsCancellationRequested && lastUpdateHiddenServicesRank.Add(Settings.Default.UpdateHiddenServicesRankDelay) < DateTime.Now)
                        {
                            //Trace.TraceInformation("Worker2Role calling UpdateHiddenServicesRank"); // verbose
                            if (!await sql.UpdateHiddenServicesRankAsync(cancellationToken))
                                lastUpdateHiddenServicesRank = DateTime.Now;
                            else
                            {
                                stillStuffToDo = true;
                                await Task.Delay(100, cancellationToken);
                            }
                        }
                        if (!cancellationToken.IsCancellationRequested && lastUpdatePageRank.Add(Settings.Default.UpdatePageRankTaskDelay) < DateTime.Now)
                        {
                            //Trace.TraceInformation("Worker2Role calling UpdatePageRank"); // verbose
                            if (!await sql.UpdatePageRankAsync(cancellationToken))
                                lastUpdatePageRank = DateTime.Now;
                            else
                            {
                                stillStuffToDo = true;
                                await Task.Delay(100, cancellationToken);
                            }
                        }
                        // post UpdatePageRankAsync
                        if (!cancellationToken.IsCancellationRequested && lasPagesPurge.Add(Settings.Default.PagesPurgeTaskDelay) < DateTime.Now)
                        {
                            //Trace.TraceInformation("Worker2Role calling PagesPurge"); // verbose
                            if (!await sql.PagesPurgeAsync(cancellationToken))
                                lasPagesPurge = DateTime.Now;
                            else
                            {
                                stillStuffToDo = true;
                                await Task.Delay(100, cancellationToken);
                            }
                        }

                        if (!cancellationToken.IsCancellationRequested && lastHDRootRescan.Add(Settings.Default.HDRootRescanDelay) < DateTime.Now)
                        {
                            //Trace.TraceInformation("Worker2Role calling HDRootRescan"); // verbose
                            foreach (string url in await sql.GetHiddenServicesListAsync(cancellationToken))
                            {
                                await StorageManager.StoreCrawleRequestAsync(url, cancellationToken);
                            }
                            lastHDRootRescan = DateTime.Now;
                            await Task.Delay(100, cancellationToken);
                        }
                    }
                    if (!stillStuffToDo)
                        await Task.Delay(10000, cancellationToken);
                }
            }
            catch (TaskCanceledException) { }
            catch (Exception ex)
            {
                Trace.TraceError("Worker2Role.RunAsync Exception : " + ex.GetBaseException().ToString());
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
            }
        }

        public override void OnStop()
        {
            Trace.TraceInformation("Worker2Role is stopping");

            this.cancellationTokenSource.Cancel();

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
