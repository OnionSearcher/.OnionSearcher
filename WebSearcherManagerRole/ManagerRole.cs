using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using WebSearcherCommon;

namespace WebSearcherManagerRole
{
    public class ManagerRole : CommonRole
    {
        private DateTime lastComputeIndexedPages;
        private DateTime lastUpdateHiddenServicesRank;
        private DateTime lasPagesPurge;
        private DateTime lastUpdatePageRank;
        private DateTime lastScanOldHD;
        private DateTime lastScanOldPages;
        private DateTime lastFrontHrefCheck;
        private DateTime lastMirrorsDetect;
        private const int mwWaitedBetweenDbWork = 500;

        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            try
            {
                PerfCounter.Init();

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
                lastFrontHrefCheck = DateTime.Now.AddMinutes(-1.0 * (
                    rnd.Next((int)Settings.Default.FrontHrefCheckDelay.TotalMinutes)
                ));
                lastMirrorsDetect = DateTime.Now.AddMinutes(-1.0 * (
                    rnd.Next((int)Settings.Default.MirrorsDetectDelay.TotalMinutes)
                ));
                lastScanOldHD = DateTime.MinValue; // Always done at startup
                lastScanOldPages = DateTime.MinValue; // Always done at startup
                
                // Url Stopper may have changed, need to purge (if more than 1k row, will be done at eatch restart)
                using (SqlManager sql = new SqlManager())
                {
                    await sql.UrlStopperPurge(cancellationToken);
                }

                DateTime end = DateTime.Now.Add(Settings.Default.TimeBeforeRecycle);

                // main loop
                while (!cancellationToken.IsCancellationRequested && (DateTime.Now < end))
                {
#if !DEBUG  // In emulator mode, don't start multiple Tor.exe
                    await TorReStarterAsync(cancellationToken); 
#endif

                    CrawlerReStarterAsync(cancellationToken);

                    bool stillStuffToDo = false;
                    using (SqlManager sql = new SqlManager()) // the connection won't be open if not used.
                    {
                        // pre UpdatePageRankAsync
                        if (!cancellationToken.IsCancellationRequested && lastComputeIndexedPages.Add(Settings.Default.ComputeIndexedPagesTaskDelay) < DateTime.Now)
                        {
                            //Trace.TraceInformation("ManagerRole calling ComputeIndexedPages"); // verbose
                            await sql.ComputeIndexedPagesAsync(cancellationToken);
                            lastComputeIndexedPages = DateTime.Now;
                            Trace.TraceInformation("ManagerRole have compute Indexed Pages");
                            await Task.Delay(mwWaitedBetweenDbWork, cancellationToken);
                        }
                        if (!cancellationToken.IsCancellationRequested && lastUpdateHiddenServicesRank.Add(Settings.Default.UpdateHiddenServicesRankDelay) < DateTime.Now)
                        {
                            //Trace.TraceInformation("ManagerRole calling UpdateHiddenServicesRank"); // verbose
                            if (!await sql.UpdateHiddenServicesRankAsync(cancellationToken))
                            {
                                lastUpdateHiddenServicesRank = DateTime.Now;
                                Trace.TraceInformation("ManagerRole have finish HD ranking");
                            }
                            else
                                stillStuffToDo = true;
                            await Task.Delay(mwWaitedBetweenDbWork, cancellationToken);
                        }
                        if (!cancellationToken.IsCancellationRequested && lastUpdatePageRank.Add(Settings.Default.UpdatePageRankTaskDelay) < DateTime.Now)
                        {
                            //Trace.TraceInformation("ManagerRole calling UpdatePageRank"); // verbose
                            if (!await sql.UpdatePageRankAsync(cancellationToken))
                            {
                                lastUpdatePageRank = DateTime.Now;
                                Trace.TraceInformation("ManagerRole have finish Pages ranking");
                            }
                            else
                                stillStuffToDo = true;
                            await Task.Delay(mwWaitedBetweenDbWork, cancellationToken);
                        }
                        // post UpdatePageRankAsync
                        if (!cancellationToken.IsCancellationRequested && lasPagesPurge.Add(Settings.Default.PagesPurgeTaskDelay) < DateTime.Now)
                        {
                            if (!await sql.PagesPurgeAsync(cancellationToken))
                            {
                                lasPagesPurge = DateTime.Now;
                                Trace.TraceInformation("ManagerRole have finish Pages purge");
                            }
                            else
                                stillStuffToDo = true;
                            await Task.Delay(mwWaitedBetweenDbWork, cancellationToken);
                        }

                        if (!cancellationToken.IsCancellationRequested && lastScanOldHD.Add(Settings.Default.ScanOldHDDelay) < DateTime.Now)
                        {
                            foreach (string url in await sql.GetHiddenServicesToCrawleAsync(cancellationToken))
                            {
                                await StorageManager.StoreOuterCrawleRequestAsync(url, true, cancellationToken);
                            }
                            lastScanOldHD = DateTime.Now;
                            Trace.TraceInformation("ManagerRole have stated a scan of old HD root");
                            await Task.Delay(mwWaitedBetweenDbWork, cancellationToken);
                        }

                        if (!cancellationToken.IsCancellationRequested && lastScanOldPages.Add(Settings.Default.ScanOldPagesDelay) < DateTime.Now)
                        {
                            foreach (string url in await sql.GetPagesToCrawleAsync(cancellationToken))
                            {
                                await StorageManager.StoreOuterCrawleRequestAsync(url, false, cancellationToken);
                            }
                            lastScanOldPages = DateTime.Now;
                            Trace.TraceInformation("ManagerRole have stated a scan of old pages");
                            await Task.Delay(mwWaitedBetweenDbWork, cancellationToken);
                        }

                        if (!cancellationToken.IsCancellationRequested && lastMirrorsDetect.Add(Settings.Default.MirrorsDetectDelay) < DateTime.Now)
                        {
                            if (!await sql.MirrorsDetectAsync(cancellationToken))
                            {
                                lastMirrorsDetect = DateTime.Now;
                                Trace.TraceInformation("ManagerRole have finish Mirrors Detect");
                            }
                            else
                                stillStuffToDo = true;
                            await Task.Delay(mwWaitedBetweenDbWork, cancellationToken);
                        }

                        // keep alive the hidden service
                        if (!cancellationToken.IsCancellationRequested && lastFrontHrefCheck.Add(Settings.Default.FrontHrefCheckDelay) < DateTime.Now)
                        {
                            await sql.UrlPurge(Settings.Default.FrontHref, cancellationToken);
                            await StorageManager.StoreOuterCrawleRequestAsync(Settings.Default.FrontHref, true, cancellationToken);
                            lastFrontHrefCheck = DateTime.Now;
                            Trace.TraceInformation("ManagerRole asked to scan HD self web service");
                            await Task.Delay(mwWaitedBetweenDbWork, cancellationToken);
                        }
                    }
                    if (!stillStuffToDo)
                        await Task.Delay(30000, cancellationToken);
                }
            }
            catch (OperationCanceledException)
            {
                Trace.TraceWarning("ManagerRole.RunAsync OperationCanceled");
            }
            catch (Exception ex)
            {
                Trace.TraceError("ManagerRole.RunAsync Exception : " + ex.GetBaseException().ToString());
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
            }
            Trace.TraceInformation("ManagerRole.RunAsync : wait for a restart");
        }

    }
}
