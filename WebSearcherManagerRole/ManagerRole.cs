using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using WebSearcherCommon;

namespace WebSearcherManagerRole
{
    public class ManagerRole : CommonRole
    {
        private DateTime nextScanOldHD;
        private DateTime nextScanOldPages;
        private DateTime nextFrontHrefCheck;
        private DateTime nextMainPerf;
        private const int mwWaitedBetweenDbWork = 500;

        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            Trace.TraceInformation("ManagerRole.RunAsync : Start");
            PerfCounter.Init();

            Random rnd = new Random();
            nextFrontHrefCheck = DateTime.Now.AddMinutes(rnd.Next((int)Settings.Default.FrontHrefCheckDelay.TotalMinutes));
            // Always done on startup
            nextScanOldHD = DateTime.MinValue;
            nextScanOldPages = DateTime.MinValue;
            nextMainPerf = DateTime.MinValue;

            // main loop
            DateTime end = DateTime.Now.Add(Settings.Default.TimeBeforeRecycle);
            while (!cancellationToken.IsCancellationRequested && (DateTime.Now < end))
            {
                try
                {
                    using (SqlManager sql = new SqlManager()) // the connection won't be open if not used.
                    {
                        if (!cancellationToken.IsCancellationRequested && nextMainPerf < DateTime.Now)
                        {
                            // TODO : move to PagesPurgeAsync with a date of the cleanup
                            PerfCounter.CounterPages.IncrementBy(await sql.ComputePerfPagesAsync(cancellationToken) - PerfCounter.CounterPages.RawValue); // direct set RawValue don't work...
                            await Task.Delay(mwWaitedBetweenDbWork, cancellationToken);
                            PerfCounter.CounterPagesOk.IncrementBy(await sql.ComputePerfPagesOkAsync(cancellationToken) - PerfCounter.CounterPagesOk.RawValue); // direct set RawValue don't work...
                            await Task.Delay(mwWaitedBetweenDbWork, cancellationToken);
                            PerfCounter.CounterHiddenServices.IncrementBy(await sql.ComputePerfHiddenServicesAsync(cancellationToken) - PerfCounter.CounterHiddenServices.RawValue); // direct set RawValue don't work...
                            await Task.Delay(mwWaitedBetweenDbWork, cancellationToken);
                            PerfCounter.CounterHiddenServicesOk.IncrementBy(await sql.ComputePerfHiddenServicesOkAsync(cancellationToken) - PerfCounter.CounterHiddenServicesOk.RawValue); // direct set RawValue don't work...
                            await Task.Delay(mwWaitedBetweenDbWork, cancellationToken);

                            nextMainPerf = DateTime.Now.Add(Settings.Default.MainPerfDelay);
                            Trace.TraceInformation("ManagerRole have computed Perf");
                        }
                        if (!cancellationToken.IsCancellationRequested && nextScanOldHD < DateTime.Now)
                        {
                            foreach (string url in await sql.GetHiddenServicesToCrawleAsync(cancellationToken))
                            {
                                await sql.CrawleRequestEnqueueAsync(url, 2, cancellationToken);
                            }
                            nextScanOldHD = DateTime.Now.Add(Settings.Default.ScanOldHDDelay);
                            Trace.TraceInformation("ManagerRole have stated a scan of old HD root");
                            await Task.Delay(mwWaitedBetweenDbWork, cancellationToken);
                        }
                        if (!cancellationToken.IsCancellationRequested && nextScanOldPages < DateTime.Now)
                        {
                            foreach (string url in await sql.GetPagesToCrawleAsync(cancellationToken))
                            {
                                await sql.CrawleRequestEnqueueAsync(url, 5, cancellationToken);
                            }
                            nextScanOldPages = DateTime.Now.Add(Settings.Default.ScanOldPagesDelay);
                            Trace.TraceInformation("ManagerRole have stated a scan of old pages");
                            await Task.Delay(mwWaitedBetweenDbWork, cancellationToken);
                        }
                        // keep alive the hidden service
                        if (!cancellationToken.IsCancellationRequested && nextFrontHrefCheck < DateTime.Now)
                        {
                            await sql.UrlPurge(Settings.Default.FrontHref + "?ping", cancellationToken);
                            await sql.CrawleRequestEnqueueAsync(Settings.Default.FrontHref + "?ping", 1, cancellationToken);
                            nextFrontHrefCheck = DateTime.Now.Add(Settings.Default.FrontHrefCheckDelay);
                            Trace.TraceInformation("ManagerRole asked to scan HD self web service");
                            await Task.Delay(mwWaitedBetweenDbWork, cancellationToken);
                        }
                    }
                    await Task.Delay(30000, cancellationToken);
                }
                catch (OperationCanceledException) { }
                catch (Exception ex)
                {
                    Trace.TraceError("ManagerRole.RunAsync Exception : " + ex.GetBaseException().ToString());
#if DEBUG
                    if (Debugger.IsAttached) { Debugger.Break(); }
#endif
                    if (!cancellationToken.IsCancellationRequested)
                        await Task.Delay(180000, cancellationToken); // busy DB usualy, don't raise alarm but retry in 3 mins
                }
            }
            Trace.TraceInformation("ManagerRole.RunAsync : End");
        }

    }
}
