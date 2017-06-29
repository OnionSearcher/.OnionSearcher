using System.Diagnostics;

namespace WebSearcherCommon
{
    public static class PerfCounter
    {
        private const string counterCategory = "WebSearcher";

        public static PerformanceCounter CounterCrawleStarted { get; private set; }
        public static PerformanceCounter CounterCrawleValided { get; private set; }
        public static void Init()
        {
            if (!PerformanceCounterCategory.Exists(counterCategory))
            {
                /* <!> Need to be done by Powershell : current right are not enouth : <Runtime executionContext="elevated" /> don't seems to work now. */
                CounterCreationDataCollection counterCollection = new CounterCreationDataCollection();
                //counterCollection.Add(new CounterCreationData()
                //{
                //    CounterName = "RoleStarted",
                //    CounterHelp = "Web Searcher Role Started",
                //    CounterType = PerformanceCounterType.NumberOfItems32
                //});
                counterCollection.Add(new CounterCreationData()
                {
                    CounterName = "CrawleStarted",
                    CounterHelp = "Web Searcher Crawle Started",
                    CounterType = PerformanceCounterType.NumberOfItems32
                });
                counterCollection.Add(new CounterCreationData()
                {
                    CounterName = "CrawleValided",
                    CounterHelp = "Web Searcher Crawle Valided",
                    CounterType = PerformanceCounterType.NumberOfItems32
                });

                /* on deb env, please run PerfCounter.cmd before as admin in order to create them ! */
                PerformanceCounterCategory.Create(
                  counterCategory,
                  "Web Searcher Category",
                  PerformanceCounterCategoryType.SingleInstance, counterCollection); // won't work ! not enouth right on azure cloud right now ! 
                
            }

            //PerformanceCounter CounterRoleStarted = new PerformanceCounter(counterCategory, "RoleStarted", string.Empty, false);
            //CounterRoleStarted.Increment(); --> Counter reseted, need to find a way to keep the value

            CounterCrawleStarted = new PerformanceCounter(counterCategory, "CrawleStarted", string.Empty, false);
            CounterCrawleValided = new PerformanceCounter(counterCategory, "CrawleValided", string.Empty, false);

        }

    }
}
