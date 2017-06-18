using com.LandonKey.SocksWebProxy;
using com.LandonKey.SocksWebProxy.Proxy;
using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;

namespace WebSearcherWorkerRole
{
    internal class ProxyManager : WebClient
    {
       private static readonly Random rand = new Random();

        public ProxyManager() : base()
        {
            // is target port free ?
            int retry = 1000;
            int portProxyHttp;
            do
            {
                do
                {
                    --retry;
                    portProxyHttp = rand.Next(1000, 65535);
                }
                while (IPGlobalProperties.GetIPGlobalProperties().GetActiveTcpListeners().Any(x => x.Port == portProxyHttp) && retry > 0);

                try
                {
                    Proxy = new SocksWebProxy(new ProxyConfig(
                        //This is an internal http->socks proxy that runs in process
                        IPAddress.Parse("127.0.0.1"),
                        //This is the port your in process http->socks proxy will run on
                        portProxyHttp,
                        //This could be an address to a local socks proxy (ex: Tor / Tor Browser, If Tor is running it will be on 127.0.0.1)
                        IPAddress.Parse("127.0.0.1"),
                        //This is the port that the socks proxy lives on (ex: Tor / Tor Browser, Tor is 9150)
                        59150,
                        //This Can be Socks4 or Socks5
                        ProxyConfig.SocksVersion.Five
                    ));
                }
                catch (ArgumentNullException ex) // Org.Mentalis.Proxy.Listener.set_ListenSocket
                {
                    Trace.TraceWarning("ProxyManager() used port : " + IPGlobalProperties.GetIPGlobalProperties().GetActiveTcpListeners().Count());
                    Trace.TraceWarning("ProxyManager() ArgumentNullException : " + ex);
                    // new proxy won't work, recycle by killin the thing.
                    Proxy = null;
                }
            } while (Proxy == null && retry > 0);

            this.DownloadProgressChanged += ProxyManager_DownloadProgressChanged;
        }

        private static void ProxyManager_DownloadProgressChanged(object sender, DownloadProgressChangedEventArgs e)
        {
            try
            {
                WebClient wc = (WebClient)sender;
                WebHeaderCollection h = wc.ResponseHeaders;
                if (h != null && h["Content-Type"] != null) // don't use AllKeys here : ofen crash for null exception during it s building
                {
                    wc.DownloadProgressChanged -= ProxyManager_DownloadProgressChanged; // stop monitoring
                    if (!h["Content-Type"].StartsWith("text/html", StringComparison.InvariantCultureIgnoreCase))
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

        protected override WebRequest GetWebRequest(Uri uri)
        {
            HttpWebRequest ret = base.GetWebRequest(uri) as HttpWebRequest;
            ret.Timeout= 120000;
            ret.Accept = "text/html"; // not enouth for filtering content response
            ret.Referer = uri.ToString(); // may avoid some ads
            //ret.UserAgent = "Mozilla/5.0";
            
            return ret;
        }
        
    }
}
