using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using HtmlAgilityPack;

namespace WebSearcherCommon
{
    public class CrawlerManager
    {

        public static async Task RunAsync(CancellationToken cancellationToken)
        {
            try
            {
                using (ProxyManager proxy = new ProxyManager()) // keep same proxy for a few time
                {
                    DateTime end = DateTime.Now.Add(Settings.Default.MaxCallPerTaskDelay);

                    while (!cancellationToken.IsCancellationRequested && end > DateTime.Now)
                    {
                        string url = await StorageManager.RetrieveCrawleRequestAsync(cancellationToken);
                        if (!string.IsNullOrEmpty(url))
                        {
                            PerfCounter.CounterCrawleStarted.Increment();
                            if (await CrawleOneAsync(proxy, url, cancellationToken))
                            {
                                PerfCounter.CounterCrawleValided.Increment();
                            }
                            else
                            {
                                // fail requeue the URL en P5
                                await StorageManager.StoreErrorCrawleRequestAsync(url, cancellationToken);
                            }
                        }
                        else // empty queue
                        {
#if DEBUG
                            await Task.Delay(20000, cancellationToken);
#else
                            await Task.Delay(60000, cancellationToken);
#endif
                        }
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                Trace.TraceError("CrawlerManager.RunAsync Exception : " + ex.GetBaseException().ToString());
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
            }
        }

        private static char[] forbiddenInHd = { '&', '\\', '\'', '\"', '\0', '\b', '\n', '\r', '\t', (char)26, '%', '_' };

        private static object concurrentHashSetLock = new object();
        /// <summary>
        /// Avoid to push several time the same URL to the queue during the lifetime of the worker ~1 h (see settings)
        /// </summary>
        private static HashSet<string> concurrentHashSet = new HashSet<string>();
        internal static void SafeAddConcurrentHashSet(string url)
        {
            lock (concurrentHashSetLock)
            {
                if (!concurrentHashSet.Contains(url))
                {
                    concurrentHashSet.Add(url);
                }
            }
        }

        internal static async Task<bool> CrawleOneAsync(ProxyManager proxy, string url, CancellationToken cancellationToken)
        {
            try
            {
                Uri uriOrig = new Uri(url); // url have been Normalized before push in the queue

                PageEntity page = new PageEntity(uriOrig);

                using (SqlManager sql = new SqlManager())
                {
                    // last scan date ?
                    if (!await sql.CheckIfCanCrawlePageAsync(page.Url, page.HiddenService, cancellationToken))
                    {
                        return true; // don't refresh !
                    }

                    string rawHtml;
                    try
                    {
                        rawHtml = await proxy.DownloadStringTaskAsync(uriOrig); // TO BIZ DON't ALLOW binary data !!
                    }
                    catch (WebException ex)
                    {
                        bool isRetry = true;
                        if (ex.Status != WebExceptionStatus.RequestCanceled)
                        {
                            if (ex.Response is HttpWebResponse err)
                            {
                                if (err.StatusCode != HttpStatusCode.NotFound)
                                {
                                    page.InnerText = err.StatusDescription; // won't be saved by PageInsertOrUpdateKo anaway...
                                }
                                else if (url != page.HiddenService) // 404 cleanup execpt domain root (who replied obviously)
                                {
                                    isRetry = false;
                                }
                            }
                            else
                            {
                                Trace.TraceInformation("CrawlerManager.CrawleOneAsync DownloadStringTaskAsync " + url + " : WebException " + ex.GetBaseException().Message);
                            }
                        }
                        else // raise by ProxyManager_DownloadProgressChanged
                        {
                            Trace.TraceInformation("CrawlerManager.CrawleOneAsync DownloadStringTaskAsync " + url + " : Cancelled");
                            isRetry = false;
                        }
                        if (isRetry)
                        {
                            await sql.PageInsertOrUpdateKo(page, cancellationToken);
                            return false;
                        }
                        else
                        {
                            await sql.UrlPurge(url, cancellationToken);
                            return true; // looks like an OK for the manager : he won't retry
                        }
                    }
                    catch (OperationCanceledException) { return false; } // retry
                    catch (Exception ex)
                    {
#if DEBUG
                        Debugger.Break();
#endif
                        Trace.TraceInformation("CrawlerManager.CrawleOneAsync DownloadStringTaskAsync " + url + " : WebException " + ex.GetBaseException().ToString());
                        await sql.PageInsertOrUpdateKo(page, cancellationToken);
                        return false;
                    }

                    HtmlDocument htmlDoc = new HtmlDocument();
                    htmlDoc.LoadHtml(rawHtml);
#if SAVEHTMLRAW
                    page.HtmlRaw = rawHtml;
#endif
                    rawHtml = null;

                    IEnumerable<HtmlNode> lst = htmlDoc.DocumentNode.Descendants("script");
                    if (lst != null)
                        lst.ToList().ForEach(x => x.Remove()); // else appear in InnerText !
                    lst = htmlDoc.DocumentNode.Descendants("style");
                    if (lst != null)
                        lst.ToList().ForEach(x => x.Remove()); // else appear in InnerText !
                    lst = htmlDoc.DocumentNode.Descendants().Where(n => n.NodeType == HtmlNodeType.Comment);
                    if (lst != null)
                        lst.ToList().ForEach(x => x.Remove()); // else appear in InnerText !
                    lst = null;

                    // Title
                    HtmlNode htmlNode2 = htmlDoc.DocumentNode.SelectSingleNode("//head/title");
                    if (htmlNode2 != null && !string.IsNullOrEmpty(htmlNode2.InnerText))
                        page.Title = PageEntity.NormalizeText(HttpUtility.HtmlDecode(htmlNode2.InnerText));
                    else
                        page.Title = uriOrig.Host;

                    // InnerText
                    htmlNode2 = htmlDoc.DocumentNode.SelectSingleNode("//body");
                    if (htmlNode2 != null && !string.IsNullOrEmpty(htmlNode2.InnerText))
                        page.InnerText = PageEntity.NormalizeText(HttpUtility.HtmlDecode(htmlNode2.InnerText));
                    else
                        page.InnerText = string.Empty; // null will raise an exception on the sql proc call
                    htmlNode2 = null;

                    // <Heading digest
                    StringBuilder heading = new StringBuilder();
                    foreach (HtmlNode htmlNode in htmlDoc.DocumentNode.Descendants("h1"))
                        if (heading.Length < SqlManager.MaxSqlIndex)
                            heading.AppendLine(htmlNode.InnerText);
                        else
                            break;
                    if (heading.Length < SqlManager.MaxSqlIndex)
                        foreach (HtmlNode htmlNode in htmlDoc.DocumentNode.Descendants("h2"))
                            if (heading.Length < SqlManager.MaxSqlIndex)
                                heading.AppendLine(htmlNode.InnerText);
                            else
                                break;
                    if (heading.Length < SqlManager.MaxSqlIndex)
                        foreach (HtmlNode htmlNode in htmlDoc.DocumentNode.Descendants("h3"))
                            if (heading.Length < SqlManager.MaxSqlIndex)
                                heading.AppendLine(htmlNode.InnerText);
                            else
                                break;
                    if (heading.Length > 0)
                        page.Heading = PageEntity.NormalizeText(HttpUtility.HtmlDecode(heading.ToString()));
                    else
                        page.Heading = string.Empty; // null will raise an exception on the sql proc call
                    heading = null;

                    // <A href digest
                    HashSet<string> innerLinks = new HashSet<string>();
                    HashSet<string> outerLinks = new HashSet<string>();
                    foreach (HtmlNode htmlNode in htmlDoc.DocumentNode.Descendants("a"))
                    {
                        if (htmlNode.Attributes.Contains("href") && !cancellationToken.IsCancellationRequested)
                        {
                            string href = htmlNode.Attributes["href"].Value;

                            Uri uriResult = null;
                            if (href.StartsWith("http"))
                            {
                                if (Uri.TryCreate(href, UriKind.Absolute, out uriResult) && uriResult.IsTor())
                                {
                                    string str = uriResult.ToString();
                                    str = PageEntity.NormalizeUrl(str);

                                    if (uriResult.DnsSafeHost != uriOrig.DnsSafeHost)
                                    {
                                        if (!outerLinks.Contains(str) && !concurrentHashSet.Contains(str))
                                        {
                                            outerLinks.Add(str);
                                            SafeAddConcurrentHashSet(str);
                                        }
                                    }
                                    else
                                    {
                                        if (!innerLinks.Contains(str) && !concurrentHashSet.Contains(str))
                                        {
                                            innerLinks.Add(str);
                                            SafeAddConcurrentHashSet(str);
                                        }
                                    }
                                }
                            }
                            else if (!href.StartsWith("#"))
                            {
                                if (Uri.TryCreate(uriOrig, href, out uriResult) && uriResult.IsTor())
                                {
                                    string str = uriResult.ToString();
                                    str = PageEntity.NormalizeUrl(str);

                                    if (!innerLinks.Contains(str) && !concurrentHashSet.Contains(str))
                                    {
                                        innerLinks.Add(str);
                                        SafeAddConcurrentHashSet(str);
                                    }
                                }
                            }
                        }
                    }
                    htmlDoc = null;

                    // finish page object before save
                    if (innerLinks.Contains(page.Url))
                        innerLinks.Remove(page.Url);
                    if (innerLinks.Contains(page.HiddenService))
                        innerLinks.Remove(page.HiddenService);

                    page.OuterHdLinks = new HashSet<string>();
                    // Ask to follow
                    foreach (string str in outerLinks)
                    {
                        string hd = PageEntity.GetHiddenService(str);
                        if (!cancellationToken.IsCancellationRequested && await sql.CheckIfCanCrawlePageAsync(str, hd, cancellationToken))
                            await StorageManager.StoreOuterCrawleRequestAsync(str, (hd == str), cancellationToken);
                        if (!page.OuterHdLinks.Contains(hd))
                        {    // basic check for sql injection
                            if (hd.Length <= 37 && hd.IndexOfAny(forbiddenInHd) == -1)
                                page.OuterHdLinks.Add(hd);
                            else
                            {
                                Trace.TraceWarning("CrawlerManager.CrawleOneAsync Strange HD outer link from " + url + " : " + hd);
#if DEBUG
                                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
                            }
                        }
                    }
                    outerLinks = null;
                    bool isLinkFromRoot = (page.HiddenService == page.Url);
                    foreach (string str in innerLinks)
                    {
                        if (!cancellationToken.IsCancellationRequested && await sql.CheckIfCanCrawlePageAsync(str, page.HiddenService, cancellationToken))
                            await StorageManager.StoreInnerCrawleRequestAsync(str, isLinkFromRoot, cancellationToken);
                    }
                    innerLinks = null;

                    try
                    {
                        await sql.PageInsertOrUpdateOk(page, cancellationToken);
                    }
                    catch (SqlException ex)
                    {
                        Trace.TraceWarning("CrawlerManager.PageInsertOrUpdateOk First try SqlException " + url + " : " + ex.GetBaseException().Message);
                        await Task.Delay(30000, cancellationToken);
                        await sql.PageInsertOrUpdateOk(page, cancellationToken); // second try...
                    }
                }

                page = null;
                return true;
            }
            catch (OperationCanceledException) { return false; }
            catch (SqlException ex)
            {
                Trace.TraceWarning("CrawlerManager.CrawleOneAsync SqlException for " + url + " : " + ex.GetBaseException().ToString());
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); } // & !ex.Message.Contains("Timeout") & !ex.Message.Contains("A transport-level error has occurred")
#endif
                return false;
            }
            catch (Exception ex)
            {
                Trace.TraceWarning("CrawlerManager.CrawleOneAsync Exception for " + url + " : " + ex.GetBaseException().ToString());
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); } // & !ex.Message.Contains("Timeout") & !ex.Message.Contains("A transport-level error has occurred")
#endif
                return false;
            }
        }

    }
}
