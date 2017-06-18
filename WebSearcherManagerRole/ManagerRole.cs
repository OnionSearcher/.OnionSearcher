using Microsoft.WindowsAzure.ServiceRuntime;
using System;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using WebSearcherCommon;

namespace WebSearcherManagerRole
{
    public class ManagerRole : RoleEntryPoint, IDisposable
    {

        private CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

        private readonly DateTime startUp = DateTime.Now;
        private DateTime lastComputeIndexedPages;
        private DateTime lastUpdateHiddenServicesRank;
        private DateTime lasPagesPurge;
        private DateTime lastUpdatePageRank;
        private DateTime lastHDRootRescan;
        private DateTime lastFrontHrefCheck;

        public override bool OnStart()
        {
            bool result = base.OnStart();

            Trace.TraceInformation("ManagerRole is starting");

            // Set the maximum number of concurrent connections
            ServicePointManager.DefaultConnectionLimit = 16;

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
            lastFrontHrefCheck = DateTime.Now.AddMinutes(-1.0 * (
                rnd.Next((int)Settings.Default.FrontHrefCheckDelay.TotalMinutes)
            ));

#if DEBUG
            // force to run one or more task
            lastFrontHrefCheck = DateTime.MinValue;
#endif

            return result;
        }

        public override void Run()
        {
            Trace.TraceInformation("ManagerRole is running");

            try
            {
                this.RunAsync(this.cancellationTokenSource.Token).Wait(this.cancellationTokenSource.Token);
            }
            catch (TaskCanceledException) { }
            catch (Exception ex)
            {
                Trace.TraceError("ManagerRole Run Exception : " + ex.GetBaseException().ToString());
            }
        }

        private const int mwWaitedBetweenDbWork = 500;

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            try
            {
                // Test empty queue if not done "normaly" immediately
                if (!cancellationToken.IsCancellationRequested)
                {
                    string oneUrl = await StorageManager.RetrieveCrawleRequestAsync(cancellationToken);
                    if (oneUrl != null)
                    {
                        await StorageManager.StoreOuterCrawleRequestAsync(oneUrl, (oneUrl == PageEntity.NormalizeUrl(oneUrl)), cancellationToken);
                    }
                    else
                    {
                        Trace.TraceWarning("ManagerRole restart scan from top Url because nothing to do !");
                        using (SqlManager sql = new SqlManager()) // the connection won't be open if not used.
                        {
                            foreach (string url in await sql.GetHiddenServicesListAsync(cancellationToken))
                            {
                                await StorageManager.StoreOuterCrawleRequestAsync(url, true, cancellationToken);
                            }
                        }
                        lastHDRootRescan = DateTime.Now;
                        await Task.Delay(mwWaitedBetweenDbWork, cancellationToken);
                    }
                }

                // Url Stopper may have changed, need to purge (if more than 1k row, will be done at eatch restart)
                using (SqlManager sql = new SqlManager())
                {
                    await sql.UrlStopperPurge(cancellationToken);
                }

                // main loop
                while (!cancellationToken.IsCancellationRequested && (startUp.Add(Settings.Default.TimeBeforeRecycle) > DateTime.Now))
                {
                    bool stillStuffToDo = false;
                    using (SqlManager sql = new SqlManager()) // the connection won't be open if not used.
                    {
                        // pre UpdatePageRankAsync
                        if (!cancellationToken.IsCancellationRequested && lastComputeIndexedPages.Add(Settings.Default.ComputeIndexedPagesTaskDelay) < DateTime.Now)
                        {
                            //Trace.TraceInformation("ManagerRole calling ComputeIndexedPages"); // verbose
                            await sql.ComputeIndexedPagesAsync(cancellationToken);
                            lastComputeIndexedPages = DateTime.Now;
                            await Task.Delay(mwWaitedBetweenDbWork, cancellationToken);
                        }
                        if (!cancellationToken.IsCancellationRequested && lastUpdateHiddenServicesRank.Add(Settings.Default.UpdateHiddenServicesRankDelay) < DateTime.Now)
                        {
                            //Trace.TraceInformation("ManagerRole calling UpdateHiddenServicesRank"); // verbose
                            if (!await sql.UpdateHiddenServicesRankAsync(cancellationToken))
                                lastUpdateHiddenServicesRank = DateTime.Now;
                            else
                                stillStuffToDo = true;
                            await Task.Delay(mwWaitedBetweenDbWork, cancellationToken);
                        }
                        if (!cancellationToken.IsCancellationRequested && lastUpdatePageRank.Add(Settings.Default.UpdatePageRankTaskDelay) < DateTime.Now)
                        {
                            //Trace.TraceInformation("ManagerRole calling UpdatePageRank"); // verbose
                            if (!await sql.UpdatePageRankAsync(cancellationToken))
                                lastUpdatePageRank = DateTime.Now;
                            else
                                stillStuffToDo = true;
                            await Task.Delay(mwWaitedBetweenDbWork, cancellationToken);
                        }
                        // post UpdatePageRankAsync
                        if (!cancellationToken.IsCancellationRequested && lasPagesPurge.Add(Settings.Default.PagesPurgeTaskDelay) < DateTime.Now)
                        {
                            //Trace.TraceInformation("ManagerRole calling PagesPurge"); // verbose
                            if (!await sql.PagesPurgeAsync(cancellationToken))
                                lasPagesPurge = DateTime.Now;
                            else
                                stillStuffToDo = true;
                            await Task.Delay(mwWaitedBetweenDbWork, cancellationToken);
                        }

                        if (!cancellationToken.IsCancellationRequested && lastHDRootRescan.Add(Settings.Default.HDRootRescanDelay) < DateTime.Now)
                        {
                            Trace.TraceInformation("ManagerRole restart scan from top Url");
                            //Trace.TraceInformation("ManagerRole calling HDRootRescan"); // verbose
                            foreach (string url in await sql.GetHiddenServicesListAsync(cancellationToken))
                            {
                                await StorageManager.StoreInnerCrawleRequestAsync(url, true, cancellationToken);
                            }
                            lastHDRootRescan = DateTime.Now;
                            await Task.Delay(mwWaitedBetweenDbWork, cancellationToken);
                        }

                        // keep alive the hidden service
                        if (!cancellationToken.IsCancellationRequested && lastFrontHrefCheck.Add(Settings.Default.FrontHrefCheckDelay) < DateTime.Now)
                        {
                            await sql.UrlPurge(Settings.Default.FrontHref, cancellationToken);
                            await StorageManager.StoreOuterCrawleRequestAsync(Settings.Default.FrontHref, true, cancellationToken);
                            lastFrontHrefCheck = DateTime.Now;
                        }
                    }
                    if (!stillStuffToDo)
                        await Task.Delay(30000, cancellationToken);
                }
                Trace.TraceInformation("ManagerRole will wait for a restart");
            }
            catch (TaskCanceledException) { }
            catch (Exception ex)
            {
                Trace.TraceError("ManagerRole.RunAsync Exception : " + ex.GetBaseException().ToString());
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
            }
        }

        public override void OnStop()
        {
            Trace.TraceInformation("ManagerRole is stopping");

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
