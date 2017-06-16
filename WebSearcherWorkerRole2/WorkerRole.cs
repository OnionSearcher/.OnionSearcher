using Microsoft.WindowsAzure.ServiceRuntime;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using WebSearcherCommon;

namespace WebSearcherWorkerRole2
{
    public class WorkerRole : RoleEntryPoint, IDisposable
    {

        private CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

        private DateTime lastComputeIndexedPages;
        private DateTime lastUpdateHiddenServicesRank;
        private DateTime lasPagesPurge;
        private DateTime lastUpdatePageRank;
        private DateTime startUp = DateTime.Now;

        public override bool OnStart()
        {
            bool result = base.OnStart();

            Trace.TraceInformation("CrawlerRole is starting");

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

#if DEBUG
            // force to run all task
            lastComputeIndexedPages = DateTime.MinValue;
            lastUpdateHiddenServicesRank = DateTime.MinValue;
            lastUpdatePageRank = DateTime.MinValue;
            lasPagesPurge = DateTime.MinValue;
#endif

            return result;
        }

        public override void Run()
        {
            Trace.TraceInformation("CrawlerRole is running");

            try
            {
                this.RunAsync(this.cancellationTokenSource.Token).Wait(this.cancellationTokenSource.Token);
            }
            catch (TaskCanceledException) { }
            catch (Exception ex)
            {
                Trace.TraceError("CrawlerRole RunAsync Exception : " + ex.GetBaseException().ToString());
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
                            await sql.ComputeIndexedPagesAsync(cancellationToken);
                            lastComputeIndexedPages = DateTime.Now;
                        }
                        if (!cancellationToken.IsCancellationRequested && lastUpdateHiddenServicesRank.Add(Settings.Default.UpdateHiddenServicesRankDelay) < DateTime.Now)
                        {
                            if (!await sql.UpdateHiddenServicesRankAsync(cancellationToken))
                                lastUpdateHiddenServicesRank = DateTime.Now;
                            else
                                stillStuffToDo = true;
                        }
                        if (!cancellationToken.IsCancellationRequested && lastUpdatePageRank.Add(Settings.Default.UpdatePageRankTaskDelay) < DateTime.Now)
                        {
                            if (!await sql.UpdatePageRankAsync(cancellationToken))
                                lastUpdatePageRank = DateTime.Now;
                            else
                                stillStuffToDo = true;
                        }
                        // post UpdatePageRankAsync
                        if (!cancellationToken.IsCancellationRequested && lasPagesPurge.Add(Settings.Default.PagesPurgeTaskDelay) < DateTime.Now)
                        {
                            if (!await sql.PagesPurgeAsync(cancellationToken))
                                lasPagesPurge = DateTime.Now;
                            else
                                stillStuffToDo = true;
                        }
                    }
                    if (!stillStuffToDo)
                        await Task.Delay(30000, cancellationToken);
                    else
                        await Task.Delay(1000, cancellationToken);
                }
            }
            catch (TaskCanceledException) { }
            catch (Exception ex)
            {
                Trace.TraceError("CrawlerRole.RunAsync Exception : " + ex.GetBaseException().ToString());
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
            }
        }

        public override void OnStop()
        {
            Trace.TraceInformation("CrawlerRole is stopping");

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
