using System;
using System.Diagnostics;
using System.Net;
using System.Net.Cache;
using System.Threading.Tasks;
using BetterHttpClient;
using WebSearcherCommon;

namespace WebSearcherWorkerRole
{
    internal class ProxyManager : IDisposable
    {
        private static readonly HttpRequestCachePolicy cachePolicy = new HttpRequestCachePolicy(HttpRequestCacheLevel.NoCacheNoStore);
        //private static readonly IPAddress localhost = IPAddress.Parse("127.0.0.1");
        //private const int ProxyPort = 60300;
        private HttpClient client;

        private static void ProxyManager_DownloadProgressChanged(object sender, DownloadProgressChangedEventArgs e)
        {
            try
            {
                WebClient wc = (WebClient)sender;
                WebHeaderCollection h = wc.ResponseHeaders;
                if (h != null && h["Content-Type"] != null) // don't use AllKeys here : ofen crash for null exception during it s building
                {
                    wc.DownloadProgressChanged -= ProxyManager_DownloadProgressChanged; // stop monitoring
                    if (!h["Content-Type"].StartsWith("text/html", StringComparison.OrdinalIgnoreCase))
                        wc.CancelAsync();
                }
            }
            catch (Exception ex)
            {
                Trace.TraceWarning("ProxyManager_DownloadProgressChanged Exception : " + ex.GetBaseException().Message);
#if DEBUG
                if (Debugger.IsAttached) Debugger.Break();
#endif
            }
        }

        public ProxyManager(int i) : base()
        {
            client = new HttpClient(new Proxy("127.0.0.1", RotManager.Rotrc0Port + i, ProxyTypeEnum.Socks))
            {
                UserAgent = "Mozilla/5.0",
                Accept = "text/html",
                NumberOfAttempts = 2,
                CachePolicy = cachePolicy,
                ValidateServerCertificateSocksProxy = false
            };
            client.DownloadProgressChanged += ProxyManager_DownloadProgressChanged;
        }

        internal async Task<string> DownloadStringTaskAsync(Uri uriOrig)
        {
            client.Referer = uriOrig.AbsoluteUri;
            return await client.DownloadStringTaskAsync(uriOrig);
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    if (client != null)
                    {
                        client.Dispose();
                        client = null;
                    }
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
