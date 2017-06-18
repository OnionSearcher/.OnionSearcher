using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace WebSearcherCommon
{

    public static class StorageManager
    {

        public static string Base64Decode(string base64EncodedData)
        {
            byte[] base64EncodedBytes = Convert.FromBase64String(base64EncodedData);
            return Encoding.UTF8.GetString(base64EncodedBytes);
        }
        public static string Base64Encode(string plainText)
        {
            byte[] plainTextBytes = Encoding.UTF8.GetBytes(plainText);
            return Convert.ToBase64String(plainTextBytes);
        }

        #region Synchrone for Web front

        private static readonly ConcurrentQueue<DailyTableEntityClass> queueHistoSearch = new ConcurrentQueue<DailyTableEntityClass>();
        private static readonly ConcurrentQueue<DailyTableEntityClass> queueHistoClick = new ConcurrentQueue<DailyTableEntityClass>();
        private static readonly ConcurrentQueue<DailyTableEntityClass> queueContact = new ConcurrentQueue<DailyTableEntityClass>();
        private static readonly ConcurrentQueue<string> queueCrawlerRequest = new ConcurrentQueue<string>();

        public static void EnqueueHistoSearch(string queryString)
        {
            queueHistoSearch.Enqueue(new DailyTableEntityClass(queryString));
        }
        public static void EnqueueHistoClick(string url)
        {
            queueHistoClick.Enqueue(new DailyTableEntityClass(url));
        }
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
                    DailyTableEntityClass dtec;
                    TableResult tr;

                    CloudTable ct = await GetHistoSearchsTableAsync(cancellationToken);
                    while (queueHistoSearch.TryDequeue(out dtec) && !cancellationToken.IsCancellationRequested)
                    {
                        tr = await ct.ExecuteAsync(TableOperation.Insert(dtec), cancellationToken);
                        if (tr.HttpStatusCode != 204)
                        {
                            Trace.TraceWarning("StorageManager.Dequeue Error " + tr.HttpStatusCode + " : " + dtec);
                        }
                    }

                    ct = await GetHistoClicksTableAsync(cancellationToken);
                    while (queueHistoClick.TryDequeue(out dtec) && !cancellationToken.IsCancellationRequested)
                    {
                        tr = await ct.ExecuteAsync(TableOperation.Insert(dtec), cancellationToken);
                        if (tr.HttpStatusCode != 204)
                        {
                            Trace.TraceWarning("StorageManager.Dequeue Error " + tr.HttpStatusCode + " : " + dtec);
                        }
                    }

                    ct = await GetContactsTableAsync(cancellationToken);
                    while (queueContact.TryDequeue(out dtec) && !cancellationToken.IsCancellationRequested)
                    {
                        tr = await ct.ExecuteAsync(TableOperation.Insert(dtec), cancellationToken);
                        if (tr.HttpStatusCode != 204)
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
                    Thread.Sleep(5000);
#else
                    Thread.Sleep(30000);
#endif
                }
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

        static private CloudStorageAccount _storageAccount;
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

        static private CloudTableClient _tableClient;
        static internal CloudTableClient GetTableClient()
        {
            if (_tableClient == null)
            {
                _tableClient = GetstorageAccount().CreateCloudTableClient();
            }
            return _tableClient;
        }

        static private CloudTable _searchHistoTable;
        static internal async Task<CloudTable> GetHistoSearchsTableAsync(CancellationToken cancellationToken)
        {
            if (_searchHistoTable == null)
            {
                _searchHistoTable = GetTableClient().GetTableReference("HistoSearchs");
                await _searchHistoTable.CreateIfNotExistsAsync(cancellationToken);
            }
            return _searchHistoTable;
        }

        static private CloudTable _clickHistoTable;
        static internal async Task<CloudTable> GetHistoClicksTableAsync(CancellationToken cancellationToken)
        {
            if (_clickHistoTable == null)
            {
                _clickHistoTable = GetTableClient().GetTableReference("HistoClicks");
                await _clickHistoTable.CreateIfNotExistsAsync(cancellationToken);
            }
            return _clickHistoTable;
        }

        static private CloudTable _contactTable;
        static internal async Task<CloudTable> GetContactsTableAsync(CancellationToken cancellationToken)
        {
            if (_contactTable == null)
            {
                _contactTable = GetTableClient().GetTableReference("ContactMessages");
                await _contactTable.CreateIfNotExistsAsync(cancellationToken);
            }
            return _contactTable;
        }

        static private CloudTable _hiddenServiceTable;
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

        static private CloudQueueClient _queueClient;
        static internal CloudQueueClient GetQueueClient()
        {
            if (_queueClient == null)
            {
                _queueClient = GetstorageAccount().CreateCloudQueueClient();
            }
            return _queueClient;
        }

        static private CloudQueue _p1CrawlerRequestP1;
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
        static private CloudQueue _p2CrawlerRequest;
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
        static private CloudQueue _p3CrawlerRequest;
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
        static private CloudQueue _p4CrawlerRequest;
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
        static private CloudQueue _p5CrawlerRequest;
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
        static public async Task StoreOuterCrawleRequestAsync(string url, bool isHiddenServiceRoot, CancellationToken cancellationToken)
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
            catch (TaskCanceledException) { }
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
        static public async Task StoreInnerCrawleRequestAsync(string url, bool isLinkedFromRoot, CancellationToken cancellationToken)
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
            catch (TaskCanceledException) { }
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
        static public async Task StoreErrorCrawleRequestAsync(string url, CancellationToken cancellationToken)
        {
            try
            {
                CloudQueue cloudQueue;
                cloudQueue = await GetP5CrawlerRequestAsync(cancellationToken);
                await cloudQueue.AddMessageAsync(new CloudQueueMessage(url), fastExpireMsg, null, null, null, cancellationToken);
            }
            catch (TaskCanceledException) { }
            catch (Exception ex)
            {
                Trace.TraceError("StorageManager.StoreErrorCrawleRequestAsync Exception for " + url + " : " + ex.GetBaseException().ToString());
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
            }
        }

        static public async Task<string> RetrieveCrawleRequestAsync(CancellationToken cancellationToken)
        {
            try
            {
                CloudQueue cloudQueue;
                CloudQueueMessage message;

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
                if (message != null)
                {
                    await cloudQueue.DeleteMessageAsync(message, cancellationToken);
                    return message.AsString;
                }
                else
                {
                    return null;
                }
            }
            catch (TaskCanceledException) { return null; }
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