using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;

namespace WebSearcherCommon
{

    public static class StorageManager
    {

        #region Synchrone for Web front

        private static readonly ConcurrentQueue<DailyTableEntityClass> queueContact = new ConcurrentQueue<DailyTableEntityClass>();
        private static readonly ConcurrentQueue<string> queueCrawlerRequest = new ConcurrentQueue<string>();

        public static void EnqueueContact(string msg)
        {
            queueContact.Enqueue(new DailyTableEntityClass(msg));
        }
        public static void EnqueueCrawlerRequest(string url)
        {
            queueCrawlerRequest.Enqueue(url);
        }

        public static async Task DequeueAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    CloudTable ct = await GetContactsTableAsync(cancellationToken);
                    while (queueContact.TryDequeue(out DailyTableEntityClass dtec) && !cancellationToken.IsCancellationRequested)
                    {
                        TableResult tr = await ct.ExecuteAsync(TableOperation.Insert(dtec), cancellationToken);
                        if (tr.HttpStatusCode == 204)
                        {

                            Trace.TraceError("StorageManager.Dequeue : You got a message !");
                        }
                        else
                        {
                            Trace.TraceWarning("StorageManager.Dequeue Error " + tr.HttpStatusCode + " : " + dtec);
                        }
                    }
                    ct = null;

                    if (!cancellationToken.IsCancellationRequested)
                    {
                        string url;
                        using (SqlManager sql = new SqlManager())
                        {
                            while (queueCrawlerRequest.TryDequeue(out url) && !cancellationToken.IsCancellationRequested)
                            {
                                url = PageEntity.NormalizeUrl(url);
                                string hd = PageEntity.GetHiddenService(url);

                                if (await sql.CheckIfCanCrawlePageAsync(url, hd, cancellationToken))
                                {
                                    await StoreOuterCrawleRequestAsync(url, (hd == url), cancellationToken);
                                }
                            }
                        }
                    }
#if DEBUG
                    await Task.Delay(5000, cancellationToken);
#else
                    await Task.Delay(30000, cancellationToken);
#endif
                }
                catch (OperationCanceledException) { }
                catch (Exception ex)
                {
                    Trace.TraceError("StorageManager.Dequeue Exception : " + ex.GetBaseException().ToString());
#if DEBUG
                    if (Debugger.IsAttached) { Debugger.Break(); }
#endif
                }
            }
        }

        // If require to debug WebRole itself
        //public static void Contact(string msg)
        //{
        //    CloudTable ct = GetContactsTableAsync(new CancellationToken()).Result;
        //    ct.Execute(TableOperation.Insert(new DailyTableEntityClass(msg)));
        //}

        #endregion Synchrone for Web front

        #region ASynchrone for internal

        private static CloudStorageAccount _storageAccount;
        internal static CloudStorageAccount GetstorageAccount()
        {
            if (_storageAccount == null)
            {
                _storageAccount = CloudStorageAccount.Parse(
                    CloudConfigurationManager.GetSetting("SharedStorage")
                );
            }
            return _storageAccount;
        }

        #region Tables

        private static CloudTableClient _tableClient;
        static internal CloudTableClient GetTableClient()
        {
            if (_tableClient == null)
            {
                _tableClient = GetstorageAccount().CreateCloudTableClient();
            }
            return _tableClient;
        }

        private static CloudTable _searchHistoTable;
        static internal async Task<CloudTable> GetHistoSearchsTableAsync(CancellationToken cancellationToken)
        {
            if (_searchHistoTable == null)
            {
                _searchHistoTable = GetTableClient().GetTableReference("HistoSearchs");
                await _searchHistoTable.CreateIfNotExistsAsync(cancellationToken);
            }
            return _searchHistoTable;
        }

        private static CloudTable _clickHistoTable;
        static internal async Task<CloudTable> GetHistoClicksTableAsync(CancellationToken cancellationToken)
        {
            if (_clickHistoTable == null)
            {
                _clickHistoTable = GetTableClient().GetTableReference("HistoClicks");
                await _clickHistoTable.CreateIfNotExistsAsync(cancellationToken);
            }
            return _clickHistoTable;
        }

        private static CloudTable _contactTable;
        static internal async Task<CloudTable> GetContactsTableAsync(CancellationToken cancellationToken)
        {
            if (_contactTable == null)
            {
                _contactTable = GetTableClient().GetTableReference("ContactMessages");
                await _contactTable.CreateIfNotExistsAsync(cancellationToken);
            }
            return _contactTable;
        }

        private static CloudTable _hiddenServiceTable;
        static internal async Task<CloudTable> GetHiddenServicesAsync(CancellationToken cancellationToken)
        {
            if (_hiddenServiceTable == null)
            {
                _hiddenServiceTable = GetTableClient().GetTableReference("HiddenServices");
                await _hiddenServiceTable.CreateIfNotExistsAsync(cancellationToken);
            }
            return _hiddenServiceTable;
        }

        #endregion Tables

        #region Queues

        private static CloudQueueClient _queueClient;
        static internal CloudQueueClient GetQueueClient()
        {
            if (_queueClient == null)
            {
                _queueClient = GetstorageAccount().CreateCloudQueueClient();
            }
            return _queueClient;
        }

        private static CloudQueue _p1CrawlerRequestP1;
        /// <summary>
        /// Hiden Service root
        /// </summary>
        static internal async Task<CloudQueue> GetP1CrawlerRequestAsync(CancellationToken cancellationToken)
        {
            if (_p1CrawlerRequestP1 == null)
            {
                _p1CrawlerRequestP1 = GetQueueClient().GetQueueReference("p1crawlerrequests");    // https://coderwall.com/p/g2xeua/gotcha-windows-azure-message-queue-400-bad-request
                await _p1CrawlerRequestP1.CreateIfNotExistsAsync(cancellationToken);
            }
            return _p1CrawlerRequestP1;
        }
        private static CloudQueue _p2CrawlerRequest;
        /// <summary>
        /// Outer link to a page refered by another Hidden Service
        /// </summary>
        static internal async Task<CloudQueue> GetP2CrawlerRequestAsync(CancellationToken cancellationToken)
        {
            if (_p2CrawlerRequest == null)
            {
                _p2CrawlerRequest = GetQueueClient().GetQueueReference("p2crawlerrequests");    // https://coderwall.com/p/g2xeua/gotcha-windows-azure-message-queue-400-bad-request
                await _p2CrawlerRequest.CreateIfNotExistsAsync(cancellationToken);
            }
            return _p2CrawlerRequest;
        }
        private static CloudQueue _p3CrawlerRequest;
        /// <summary>
        /// Inner link from the root of the hidden service
        /// </summary>
        static internal async Task<CloudQueue> GetP3CrawlerRequestAsync(CancellationToken cancellationToken)
        {
            if (_p3CrawlerRequest == null)
            {
                _p3CrawlerRequest = GetQueueClient().GetQueueReference("p3crawlerrequests");    // https://coderwall.com/p/g2xeua/gotcha-windows-azure-message-queue-400-bad-request
                await _p3CrawlerRequest.CreateIfNotExistsAsync(cancellationToken);
            }
            return _p3CrawlerRequest;
        }
        private static CloudQueue _p4CrawlerRequest;
        /// <summary>
        /// Inner link from another lambda page
        /// </summary>
        static internal async Task<CloudQueue> GetP4CrawlerRequestAsync(CancellationToken cancellationToken)
        {
            if (_p4CrawlerRequest == null)
            {
                _p4CrawlerRequest = GetQueueClient().GetQueueReference("p4crawlerrequests");    // https://coderwall.com/p/g2xeua/gotcha-windows-azure-message-queue-400-bad-request
                await _p4CrawlerRequest.CreateIfNotExistsAsync(cancellationToken);
            }
            return _p4CrawlerRequest;
        }
        private static CloudQueue _p5CrawlerRequest;
        /// <summary>
        /// Error pages
        /// </summary>
        static internal async Task<CloudQueue> GetP5CrawlerRequestAsync(CancellationToken cancellationToken)
        {
            if (_p5CrawlerRequest == null)
            {
                _p5CrawlerRequest = GetQueueClient().GetQueueReference("p5crawlerrequests");    // https://coderwall.com/p/g2xeua/gotcha-windows-azure-message-queue-500-bad-request
                await _p5CrawlerRequest.CreateIfNotExistsAsync(cancellationToken);
            }
            return _p5CrawlerRequest;
        }

        #endregion Queues

        #endregion ASynchrone for internal

        #region ASynchrone for worker

        /// <summary>
        /// Priority 1 for root, else 2
        /// </summary>
        public static async Task StoreOuterCrawleRequestAsync(string url, bool isHiddenServiceRoot, CancellationToken cancellationToken)
        {
            try
            {
                CloudQueue cloudQueue;
                if (isHiddenServiceRoot)
                    cloudQueue = await GetP1CrawlerRequestAsync(cancellationToken);
                else
                    cloudQueue = await GetP2CrawlerRequestAsync(cancellationToken);

                await cloudQueue.AddMessageAsync(new CloudQueueMessage(url), cancellationToken);
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                Trace.TraceError("StorageManager.StoreOuterCrawleRequestAsync Exception for " + url + " : " + ex.GetBaseException().ToString());
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
            }
        }

        private static readonly TimeSpan fastExpireMsg = TimeSpan.FromHours(4.0); // limit queue growing endlessly
        /// <summary>
        /// Priority 3 if refered from HD root, 4 else
        /// </summary>
        public static async Task StoreInnerCrawleRequestAsync(string url, bool isLinkedFromRoot, CancellationToken cancellationToken)
        {
            try
            {
                CloudQueue cloudQueue;
                if (isLinkedFromRoot)
                    cloudQueue = await GetP3CrawlerRequestAsync(cancellationToken);
                else
                    cloudQueue = await GetP4CrawlerRequestAsync(cancellationToken);
                await cloudQueue.AddMessageAsync(new CloudQueueMessage(url), fastExpireMsg, null, null, null, cancellationToken);
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                Trace.TraceError("StorageManager.StoreInnerCrawleRequestAsync Exception for " + url + " : " + ex.GetBaseException().ToString());
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
            }
        }

        /// <summary>
        /// Priority 3 if refered from HD root, 4 else
        /// </summary>
        public static async Task StoreErrorCrawleRequestAsync(string url, CancellationToken cancellationToken)
        {
            try
            {
                CloudQueue cloudQueue;
                cloudQueue = await GetP5CrawlerRequestAsync(cancellationToken);
                await cloudQueue.AddMessageAsync(new CloudQueueMessage(url), fastExpireMsg, null, null, null, cancellationToken);
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                Trace.TraceError("StorageManager.StoreErrorCrawleRequestAsync Exception for " + url + " : " + ex.GetBaseException().ToString());
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
            }
        }

        // avoid requesting each time the P1 & P2 uselessly
        private static readonly object lockLastCloudQueueMode = new object();
        private static readonly TimeSpan retrieveCrawleRequestSaved = Settings.Default.RetrieveCrawleRequestSaved;
        private static bool isLastCloudQueueMode = false;
        private static DateTime lastRetrieveCrawleRequestSaved = DateTime.MinValue;
        private static CloudQueue lastCloudQueue; // <!> don t set to null in loop

        public static async Task<string> RetrieveCrawleRequestAsync(CancellationToken cancellationToken)
        {
            try
            {
                CloudQueue cloudQueue = null;
                CloudQueueMessage message = null;

                // cache manager
                if (isLastCloudQueueMode) // watch out for multi task 
                {
                    cloudQueue = lastCloudQueue; // for delete , have to be the same than the get()
                    message = await cloudQueue.GetMessageAsync(cancellationToken);
                    if (message == null || lastRetrieveCrawleRequestSaved.Add(retrieveCrawleRequestSaved) < DateTime.Now) // if not result in current queue or cache time expired
                        lock (lockLastCloudQueueMode)
                        {
                            if (isLastCloudQueueMode)
                                isLastCloudQueueMode = false; // reset cache
                            Debug.WriteLine("isLastCloudQueueMode = false");
                        }
                }
                // normal process
                if (message == null)
                {
                    cloudQueue = await GetP1CrawlerRequestAsync(cancellationToken);
                    message = await cloudQueue.GetMessageAsync(cancellationToken);
                    if (message == null)
                    {
                        cloudQueue = await GetP2CrawlerRequestAsync(cancellationToken);
                        message = await cloudQueue.GetMessageAsync(cancellationToken);
                        if (message == null)
                        {
                            cloudQueue = await GetP3CrawlerRequestAsync(cancellationToken);
                            message = await cloudQueue.GetMessageAsync(cancellationToken);
                            if (message == null)
                            {
                                cloudQueue = await GetP4CrawlerRequestAsync(cancellationToken);
                                message = await cloudQueue.GetMessageAsync(cancellationToken);
                                if (message == null)
                                {
                                    cloudQueue = await GetP5CrawlerRequestAsync(cancellationToken);
                                    message = await cloudQueue.GetMessageAsync(cancellationToken);
                                }
                            }
                        }
                    }
                }
                if (message != null)
                {
                    // queue cache saver
                    if (!isLastCloudQueueMode) // multitask may overwrite themself : don t care here
                    {
                        lock (lockLastCloudQueueMode)
                        {
                            if (!isLastCloudQueueMode)
                            {
                                lastCloudQueue = cloudQueue; // before setting isLastCloudQueueMode
                                lastRetrieveCrawleRequestSaved = DateTime.Now;
                                isLastCloudQueueMode = true; // after setting lastCloudQueue
                                Debug.WriteLine("isLastCloudQueueMode = true");
                            }
                        }
                    }

                    // process 
                    await cloudQueue.DeleteMessageAsync(message, cancellationToken);
                    return message.AsString;
                }
                else
                {
                    return null;
                }
            }
            catch (OperationCanceledException) { return null; }
            catch (Exception ex)
            {
                Trace.TraceError("StorageManager.RetrieveCrawleRequestAsync Exception : " + ex.GetBaseException().ToString());
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
                return null;
            }
        }

        #endregion ASynchrone for worker

    }
}