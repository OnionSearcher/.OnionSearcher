using System.Diagnostics;

namespace WebSearcherManagerRole
{
    public static class PerfCounter
    {
        private const string counterCategory = "WebSearcherManager";

        internal static PerformanceCounter CounterPages;
        internal static PerformanceCounter CounterPagesOk;
        internal static PerformanceCounter CounterHiddenServices;
        internal static PerformanceCounter CounterHiddenServicesOk;

        public static void Init()
        {
            /* <!> Need to be done by Powershell : current right are not enouth : <Runtime executionContext="elevated" /> don't seems to work now.*/
            if (!PerformanceCounterCategory.Exists(counterCategory))
            {
                CounterCreationDataCollection counterCollection = new CounterCreationDataCollection
                {
                    new CounterCreationData()
                    {
                        CounterName = "Pages",
                        CounterHelp = "Web Searcher Pages",
                        CounterType = PerformanceCounterType.NumberOfItems32
                    },
                    new CounterCreationData()
                    {
                        CounterName = "PagesOk",
                        CounterHelp = "Web Searcher Pages OK",
                        CounterType = PerformanceCounterType.NumberOfItems32
                    },
                    new CounterCreationData()
                    {
                        CounterName = "HiddenServices",
                        CounterHelp = "Web Searcher Hidden Services",
                        CounterType = PerformanceCounterType.NumberOfItems32
                    },
                    new CounterCreationData()
                    {
                        CounterName = "HiddenServicesOk",
                        CounterHelp = "Web Searcher Hidden Services OK",
                        CounterType = PerformanceCounterType.NumberOfItems32
                    }
                };
                PerformanceCounterCategory.Create(
                  counterCategory,
                  "Web Searcher Category",
                  PerformanceCounterCategoryType.SingleInstance, counterCollection); // won't work ! not enouth right on azure cloud right now ! 
            }

            CounterPages = new PerformanceCounter(counterCategory, "Pages", string.Empty, false);
            CounterPagesOk = new PerformanceCounter(counterCategory, "PagesOk", string.Empty, false);
            CounterHiddenServices = new PerformanceCounter(counterCategory, "HiddenServices", string.Empty, false);
            CounterHiddenServicesOk = new PerformanceCounter(counterCategory, "HiddenServicesOk", string.Empty, false);
        }

    }
}
