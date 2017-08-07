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
using WebSearcherCommon;

namespace WebSearcherWorkerRole
{
    internal static class CrawleManager
    {

        private static char[] forbiddenInHd = { '&', '\\', '\'', '\"', '\0', '\b', '\n', '\r', '\t', (char)26, '%', '_' };
        

        internal static async Task<bool> CrawleOneAsync(ProxyManager proxy, string url, CancellationToken cancellationToken)
        {
            try
            {
                Uri uriOrig = new Uri(url); // url have been Normalized before push in the queue
                PageEntity page = new PageEntity(uriOrig);

                using (SqlManager sql = new SqlManager())
                {
                    string rawHtml;
                    try
                    {
                        rawHtml = await proxy.DownloadStringTaskAsync(uriOrig);
                    }
                    catch (WebException ex)
                    {
                        if (!cancellationToken.IsCancellationRequested)
                        {
                            bool isRetry = true;
                            if (ex.Status != WebExceptionStatus.RequestCanceled)
                            {
                                if (ex.Response is HttpWebResponse err)
                                {
                                    if (err.StatusCode != HttpStatusCode.NotFound)
                                        page.InnerText = err.StatusDescription; // won't be saved by PageInsertOrUpdateKo anaway...
                                    else if (url != page.HiddenService) // 404 cleanup execpt domain root (who replied obviously)
                                        isRetry = false;
                                }
                                else
                                    Trace.TraceInformation("CrawleManager.CrawleOneAsync DownloadStringTaskAsync " + url + " : WebException " + ex.GetBaseException().Message);
                            }
                            else // raise by ProxyManager_DownloadProgressChanged
                            {
                                Trace.TraceInformation("CrawleManager.CrawleOneAsync DownloadStringTaskAsync " + url + " : Cancelled");
                                isRetry = false;
                            }
                            if (isRetry)
                            {
                                await sql.PageUpdateKo(page, cancellationToken);
                                return false;
                            }
                            else
                            {
                                await sql.UrlPurge(url, cancellationToken);
                                return true; // looks like an OK for the manager : he won't retry
                            }
                        }
                        else
                        {
                            return false;
                        }
                    }
                    catch (OperationCanceledException) { return false; } // retry
                    catch (Exception ex)
                    {
                        Trace.TraceInformation("CrawleManager.CrawleOneAsync DownloadStringTaskAsync " + url + " : WebException " + ex.GetBaseException().ToString());
                        if (!cancellationToken.IsCancellationRequested)
                        {
#if DEBUG
                            Debugger.Break();
#endif
                            await sql.PageUpdateKo(page, cancellationToken);
                        }
                        return false;
                    }

                    HtmlDocument htmlDoc = new HtmlDocument();
                    htmlDoc.LoadHtml(rawHtml);
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
                        page.InnerText = string.Empty;  // a null will raise an exception on the sql proc call
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
                    page.OuterLinks = new HashSet<string>();
                    page.OuterHdLinks = new HashSet<string>();
                    page.InnerLinks = new HashSet<string>();
                    foreach (HtmlNode htmlNode in htmlDoc.DocumentNode.Descendants("a"))
                    {
                        if (htmlNode.Attributes.Contains("href") && !cancellationToken.IsCancellationRequested)
                        {
                            string href = htmlNode.Attributes["href"].Value;

                            Uri uriResult = null;
                            if (href.StartsWith("http"))
                            {
                                if (Uri.TryCreate(href, UriKind.Absolute, out uriResult) && PageEntity.IsTorUri(uriResult)) // >=29 because some funny link with just: href='http://'
                                {
                                    string str = uriResult.ToString();
                                    str = PageEntity.NormalizeUrl(str);
                                    if (str.Length < SqlManager.MaxSqlIndex)
                                        if (uriResult.DnsSafeHost != uriOrig.DnsSafeHost)
                                        {
                                            if (!page.OuterLinks.Contains(str) && !page.OuterHdLinks.Contains(str))
                                            {
                                                string hd = PageEntity.GetHiddenService(str);    // Digest outter HD
                                                if (hd.Length <= 37 && hd.IndexOfAny(forbiddenInHd) == -1) // some strange url may cause injection
                                                {
                                                    if(hd == str)
                                                        page.OuterHdLinks.Add(hd);
                                                    else
                                                        page.OuterLinks.Add(str);
                                                }
                                                else
                                                {
                                                    Trace.TraceWarning("CrawleManager.CrawleOneAsync Strange HD outer link from " + url + " : " + hd);
#if DEBUG
                                                    if (Debugger.IsAttached) { Debugger.Break(); }
#endif
                                                }
                                            }
                                        }
                                        else if (!page.InnerLinks.Contains(str))
                                            page.InnerLinks.Add(str);
                                }
                            }
                            else if (!href.StartsWith("#"))
                            {
                                if (Uri.TryCreate(uriOrig, href, out uriResult) && PageEntity.IsTorUri(uriResult))
                                {
                                    string str = uriResult.ToString();
                                    str = PageEntity.NormalizeUrl(str);

                                    if (str.Length < SqlManager.MaxSqlIndex && !page.InnerLinks.Contains(str))
                                    {
                                        page.InnerLinks.Add(str);
                                    }
                                }
                            }
                        }
                    }
                    htmlDoc = null;
                    if (page.InnerLinks.Contains(page.Url)) page.InnerLinks.Remove(page.Url);
                    if (page.InnerLinks.Contains(page.HiddenService)) page.InnerLinks.Remove(page.HiddenService);
                    
                    // Page Update
                    //try
                    //{
                    await sql.PageInsertOrUpdateOk(page, cancellationToken);
                    //}
                    //catch (SqlException ex)
                    //{
                    //    Trace.TraceWarning("CrawleManager.PageInsertOrUpdateOk First try SqlException " + url + " : " + ex.GetBaseException().Message);
                    //    await Task.Delay(30000, cancellationToken);
                    //    await sql.PageInsertOrUpdateOk(page, cancellationToken); // second try...
                    //}
                }

                page = null;
                return true;
            }
            catch (OperationCanceledException) { return false; }
            catch (SqlException ex)
            {
                Exception exb = ex.GetBaseException();
                if (!exb.Message.Contains("Operation cancelled by user."))
                {
                    Trace.TraceWarning("CrawleManager.CrawleOneAsync SqlException : " + ex.GetBaseException().ToString());
#if DEBUG
                    if (Debugger.IsAttached) { Debugger.Break(); }
#endif
                } // <=> catch (OperationCanceledException)
                return false; // will retry and not save the page in failed
            }
            catch (Exception ex)
            {
                Trace.TraceWarning("CrawleManager.CrawleOneAsync Exception for " + url + " : " + ex.GetBaseException().ToString());
#if DEBUG
                if (Debugger.IsAttached) { Debugger.Break(); }
#endif
                return false; // will retry and not save the page in failed
            }
        }

    }
}
