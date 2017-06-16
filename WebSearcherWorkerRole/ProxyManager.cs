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

        public ProxyManager() : base()
        {
            // is target port free ? 
            Random rand = new Random();
            int portProxyHttp;
            do
            {
                portProxyHttp = rand.Next(1000, 65535);
            }
            while (IPGlobalProperties.GetIPGlobalProperties().GetActiveTcpListeners().Any(x => x.Port == portProxyHttp));

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
                    33150,
                    //This Can be Socks4 or Socks5
                    ProxyConfig.SocksVersion.Five
                ));
            }
            catch (ArgumentNullException ex)
            {
                Trace.TraceWarning("ProxyManager() used port : " + IPGlobalProperties.GetIPGlobalProperties().GetActiveTcpListeners().Count());
                Trace.TraceWarning("ProxyManager() ArgumentNullException : " + ex);
            }

        }

    }
}
