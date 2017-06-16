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
                        CloudQueue cloudQueue = await GetCrawlerRequestAsync(cancellationToken);
                        using (SqlManager sql = new SqlManager())
                        {
                            while (queueCrawlerRequest.TryDequeue(out url) && !cancellationToken.IsCancellationRequested)
                            {
                                PageEntity.NormalizeUrl(url);
                                if (await sql.CheckIfCanCrawlePageAsync(url, cancellationToken))
                                {
                                    await cloudQueue.AddMessageAsync(new CloudQueueMessage(url), cancellationToken);
                                }
                            }
                        }
                    }
#if DEBUG
                    Thread.Sleep(5000);
#else
                    Thread.Sleep(60000);
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


        public static void Contact(string msg)
        {
            CloudTable ct = GetContactsTableAsync(new CancellationToken()).Result;
            ct.Execute(TableOperation.Insert(new DailyTableEntityClass(msg)));
        }

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

        static private CloudQueue _crawlerRequest;
        static internal async Task<CloudQueue> GetCrawlerRequestAsync(CancellationToken cancellationToken)
        {
            if (_crawlerRequest == null)
            {
                _crawlerRequest = GetQueueClient().GetQueueReference("crawlerrequests");    // https://coderwall.com/p/g2xeua/gotcha-windows-azure-message-queue-400-bad-request
                await _crawlerRequest.CreateIfNotExistsAsync(cancellationToken);
            }
            return _crawlerRequest;
        }

        #endregion Queues

        #endregion ASynchrone for internal

        #region ASynchrone for worker

        static public async Task<bool> HasCrawleRequestValueAsync(CancellationToken cancellationToken)
        {
            try
            {
                CloudQueue cloudQueue = await GetCrawlerRequestAsync(cancellationToken);
                await cloudQueue.FetchAttributesAsync(cancellationToken); // in order to compute cloudQueue.ApproximateMessageCount
                int? i = cloudQueue.ApproximateMessageCount;
                Trace.TraceInformation("StorageManager.HasCrawleRequestValueAsync : CrawlerRequestQueue.ApproximateMessageCount = " + (i ?? 0));
                return i.HasValue && i.Value > 0;
            }
            catch (Exception ex)
            {
                Trace.TraceError("StorageManager.HasCrawleRequestValueAsync Exception : " + ex.GetBaseException().ToString());
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
                return true; // avoid creation of initial scope
            }
        }

        static public async Task StoreCrawleRequestAsync(string url, CancellationToken cancellationToken)
        {
            try
            {
                CloudQueue cloudQueue = await GetCrawlerRequestAsync(cancellationToken);
                await cloudQueue.AddMessageAsync(new CloudQueueMessage(url), cancellationToken);
            }
            catch (TaskCanceledException) { }
            catch (Exception ex)
            {
                Trace.TraceError("StorageManager.StoreCrawleRequestAsync Exception for " + url + " : " + ex.GetBaseException().ToString());
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
            }
        }

        static public async Task<string> RetrieveCrawleRequestAsync(CancellationToken cancellationToken)
        {
            try
            {
                CloudQueue cloudQueue = await GetCrawlerRequestAsync(cancellationToken);
                CloudQueueMessage message = await cloudQueue.GetMessageAsync(cancellationToken);
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