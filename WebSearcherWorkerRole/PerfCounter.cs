using System.Diagnostics;

namespace WebSearcherWorkerRole
{
    public static class PerfCounter
    {
        private const string counterCategory = "WebSearcherWorker";

        internal static PerformanceCounter CounterCrawleStarted;
        internal static PerformanceCounter CounterCrawleValided;

        public static void Init()
        {
            /* <!> Need to be done by Powershell : current right are not enouth : <Runtime executionContext="elevated" /> don't seems to work now. */
            if (!PerformanceCounterCategory.Exists(counterCategory))
            {
                CounterCreationDataCollection counterCollection = new CounterCreationDataCollection
                {
                    new CounterCreationData()
                    {
                        CounterName = "CrawleStarted",
                        CounterHelp = "Web Searcher Crawle Started",
                        CounterType = PerformanceCounterType.NumberOfItems32
                    },
                    new CounterCreationData()
                    {
                        CounterName = "CrawleValided",
                        CounterHelp = "Web Searcher Crawle Valided",
                        CounterType = PerformanceCounterType.NumberOfItems32
                    }
                };
                PerformanceCounterCategory.Create(
                  counterCategory,
                  "Web Searcher Category",
                  PerformanceCounterCategoryType.SingleInstance, counterCollection); // won't work ! not enouth right on azure cloud right now ! 
            }

            CounterCrawleStarted = new PerformanceCounter(counterCategory, "CrawleStarted", string.Empty, false);
            CounterCrawleValided = new PerformanceCounter(counterCategory, "CrawleValided", string.Empty, false);
            CounterCrawleStarted.RawValue = 0; // show that it s a new run by starting @0
            CounterCrawleValided.RawValue = 0; // show that it s a new run by starting @0
        }
        
    }
}
