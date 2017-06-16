using HtmlAgilityPack;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using WebSearcherCommon;

namespace WebSearcherWorkerRole
{
    internal class CrawlerManager
    {

        internal static async Task RunAsync(CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    string url = await StorageManager.RetrieveCrawleRequestAsync(cancellationToken);
                    if (!string.IsNullOrEmpty(url))
                    {
                        if (!(await CrawleOneAsync(url, cancellationToken)))
                        {
                            // fail requeue the URL
                            await StorageManager.StoreCrawleRequestAsync(url, cancellationToken);
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
            catch (TaskCanceledException) { }
            catch (Exception ex)
            {
                Trace.TraceError("CrawlerManager.RunAsync Exception : " + ex.GetBaseException().ToString());
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
            }
        }
        
        internal static async Task<bool> CrawleOneAsync(string url, CancellationToken cancellationToken)
        {
            try
            {
                Uri uriOrig = new Uri(url); // url have been Normalized before push in the queue
                
                PageEntity page = new PageEntity(uriOrig);

                using (SqlManager sql = new SqlManager())
                {
                    // last scan date ?
                    if (!await sql.CheckIfCanCrawlePageAsync(url, cancellationToken))
                    {
                        return true; // don't refresh !
                    }

                    string rawHtml;
                    try
                    {
                        using (ProxyManager proxy = new ProxyManager())
                        {
                            rawHtml = await proxy.DownloadStringTaskAsync(uriOrig);
                        }
                    }
                    catch (WebException ex)
                    {
                        HttpWebResponse err = ex.Response as HttpWebResponse;
                        if (err != null)
                        {
                            page.InnerText = err.StatusDescription;
                        }
                        else
                        {
                            Trace.TraceWarning("CrawlerManager.CrawleOneAsync DownloadStringTaskAsync " + url + " : Error " + ex.GetBaseException().Message);
                        }

                        await sql.PageInsertOrUpdateKo(page, cancellationToken); // only used for follow the connection issue with the host, not crawler inernal issue
                        return false;
                    }
                    
                    HtmlDocument htmlDoc = new HtmlDocument();
                    htmlDoc.LoadHtml(rawHtml);
#if SAVEHTMLRAW
                    page.HtmlRaw = rawHtml;
#endif
                    rawHtml = null;
                    htmlDoc.DocumentNode.Descendants("script").ToList().ForEach(x => x.Remove()); // else appear in InnerText !
                    htmlDoc.DocumentNode.Descendants("style").ToList().ForEach(x => x.Remove()); // else appear in InnerText !
                    htmlDoc.DocumentNode.Descendants().Where(n => n.NodeType == HtmlNodeType.Comment).ToList().ForEach(x => x.Remove()); // else appear in InnerText !

                    // Title
                    HtmlNode htmlNode2 = htmlDoc.DocumentNode.SelectSingleNode("//head/title");
                    if (!string.IsNullOrEmpty(page.InnerText))
                        page.Title = PageEntity.NormalizeText(HttpUtility.HtmlDecode(htmlNode2.InnerText));
                    else
                        page.Title = uriOrig.Host;
                    // InnerText
                    htmlNode2 = htmlDoc.DocumentNode.SelectSingleNode("//body");
                    if (!string.IsNullOrEmpty(page.InnerText))
                        page.InnerText = PageEntity.NormalizeText(HttpUtility.HtmlDecode(htmlNode2.InnerText));
                    else
                        page.InnerText = string.Empty; // null will raise an exception on the sql proc call
                    htmlNode2 = null;
                    
                    // <A href digest
                    page.InnerLinks = new HashSet<string>();
                    page.OuterLinks = new HashSet<string>();
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
                                        if (!page.OuterLinks.Contains(str))
                                            page.OuterLinks.Add(str);
                                    }
                                    else
                                    {
                                        if (!page.InnerLinks.Contains(str))
                                            page.InnerLinks.Add(str);
                                    }
                                }
                            }
                            else if (!href.StartsWith("#"))
                            {
                                if (Uri.TryCreate(uriOrig, href, out uriResult) && uriResult.IsTor())
                                {
                                    string str = uriResult.ToString();
                                    str = PageEntity.NormalizeUrl(str);
                                    
                                    if (!page.InnerLinks.Contains(str))
                                        page.InnerLinks.Add(str);
                                }
                            }
                        }
                    }
                    htmlDoc = null;

                    // finish page object before save
                    if (page.InnerLinks.Contains(page.Url))
                        page.InnerLinks.Remove(page.Url); // remove self reference (orinaly with #)

                    await sql.PageInsertOrUpdateOk(page, cancellationToken);

                    // Ask to follow
                    foreach (string str in page.OuterLinks)
                    {
                        if (!cancellationToken.IsCancellationRequested && await sql.CheckIfCanCrawlePageAsync(str, cancellationToken))
                            await StorageManager.StoreCrawleRequestAsync(str, cancellationToken);
                    }

                    foreach (string str in page.InnerLinks)
                    {
                        if (!cancellationToken.IsCancellationRequested && await sql.CheckIfCanCrawlePageAsync(str, cancellationToken))
                            await StorageManager.StoreCrawleRequestAsync(str, cancellationToken);
                    }
                }

                page = null;
                return true;
            }
            catch (TaskCanceledException) { return false; }
            catch (Exception ex)
            {
                Trace.TraceError("CrawlerManager.CrawleOneAsync Exception for " + url + " : " + ex.GetBaseException().ToString());
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
                return false;
            }
        }
        
    }
}
