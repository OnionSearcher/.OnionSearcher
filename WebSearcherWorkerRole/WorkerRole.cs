using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime;
using System.Threading;
using System.Threading.Tasks;
using HtmlAgilityPack;
using WebSearcherCommon;

namespace WebSearcherWorkerRole
{
    public class WorkerRole : CommonRole
    {
        private static readonly int maxCallPerTask = Settings.Default.MaxCallPerTask;
        private List<Task> taskPool;

        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            Trace.TraceInformation("WorkerRole.RunAsync : Start");
            try
            {
                PerfCounter.Init();
                RotManager.TryKillTorIfRequired();

                // pools init
#if DEBUG
                taskPool = new List<Task>(Settings.Default.NbCrawlersPerInstance / 2);
#else
                taskPool = new List<Task>(Settings.Default.NbCrawlersPerInstance);
#endif
                for (int i = 0; i < taskPool.Capacity; i++)
                    taskPool.Add(null);
                // NormalizeUrl Init
                using (SqlManager sql = new SqlManager())
                {
                    UriManager.NormalizeUrlInit(sql);
                }
                int gCFullCollectRemainingMin = Settings.Default.GCFullCollectMin;
                HtmlDocument.MaxDepthLevel = Int16.MaxValue; // default value Int32.MaxValue is far too hight : an call stack exeption will be raised far before (around 43k)...

                // main loop
                while (!cancellationToken.IsCancellationRequested)
                {
                    for (int i = 0; !cancellationToken.IsCancellationRequested && i < taskPool.Count; i++)
                    {
                        Task task = taskPool[i];
                        if (task != null && (task.IsCanceled || task.IsCompleted || task.IsFaulted))
                        {
                            task.Dispose();
                            taskPool[i] = null;
                        }
                        if (taskPool[i] == null && !cancellationToken.IsCancellationRequested)
                        {
                            int iVarRequiredForLambda = i; // <!> else the i may be changed by next for iteration in this multi task app !!!
                            taskPool[i] = Task.Run(() =>
                                {
                                    RunTask(iVarRequiredForLambda, cancellationToken).Wait(); // cancel supported by task, so not used for the wait()
                                }, cancellationToken);
                            await Task.Delay(1000, cancellationToken); // avoid violent startup by x tor started in same instant
                        }
                    }
                    await Task.Delay(60000, cancellationToken);    // 1 min
                    // We use a lot of large object for short time, si need to change the GC default mode
                    gCFullCollectRemainingMin--;
                    if (!cancellationToken.IsCancellationRequested && gCFullCollectRemainingMin == 0)
                    {
                        gCFullCollectRemainingMin = Settings.Default.GCFullCollectMin;
                        GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce; // https://msdn.microsoft.com/en-us/library/system.runtime.gcsettings.largeobjectheapcompactionmode(v=vs.110).aspx
                        GC.Collect();   // take some tome for freeing RAM and reduce LargeObjectHeap fragment
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (AggregateException) { }
            catch (Exception ex)
            {
                Trace.TraceError("WorkerRole.RunAsync Exception : " + ex.GetBaseException().ToString());
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
            }

            Trace.TraceInformation("WorkerRole.RunAsync : End");
        }

        private static async Task RunTask(int i, CancellationToken cancellationToken)
        {
            try
            {
                //await RotManager.WaitFreePort(RotManager.Rotrc0Port + i, cancellationToken);
                using (RotManager rot = new RotManager(i)) // keep same proxy for a few time
                {
                    await rot.WaitStartAsync(cancellationToken); // wait tor for starting the crawling
                    using (ProxyManager proxy = new ProxyManager(i)) // synchro proxy recycle and Rot restart
                    using (SqlManager sql = new SqlManager())
                    {
                        for (int end = 0; !cancellationToken.IsCancellationRequested && rot.IsProcessOk() && end < maxCallPerTask; end++) // need to determine how many "stable request" can be made before having the "same result for different url" issue
                        {
                            string url;
                            url = await sql.CrawleRequestDequeueAsync(cancellationToken);
                            if (!cancellationToken.IsCancellationRequested)
                            {
                                if (!string.IsNullOrEmpty(url))
                                {
                                    PerfCounter.CounterCrawleStarted.Increment();
                                    if (await CrawleManager.CrawleOneAsync(proxy, url, sql, cancellationToken))
                                        PerfCounter.CounterCrawleValided.Increment();
                                    else if (!cancellationToken.IsCancellationRequested)  // fail requeue the URL en P5
                                        await sql.CrawleRequestEnqueueAsync(url, 6, cancellationToken);
                                }
                                else // empty queue (what a dream!)
                                    await Task.Delay(1000, cancellationToken);
                            }
                        }
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                Trace.TraceError("WorkerRole.RunTask Exception : " + ex.GetBaseException().ToString());
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
            }
        }

        public override void OnStop()
        {
            base.OnStop(); // raise the cancellationToken before the taskPool cleanup

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
                taskPool = null; // may raise exception on the i < taskPool.Count who may continue in parrallele
            }
        }

    }
}
