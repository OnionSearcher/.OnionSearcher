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
using BetterHttpClient.Socks;
using HtmlAgilityPack;
using WebSearcherCommon;

namespace WebSearcherWorkerRole
{
    internal static class CrawleManager
    {

        private static char[] forbiddenInHd = { '&', '\\', '\'', '\"', '\0', '\b', '\n', '\r', '\t', (char)26, '%', '_' };

        internal static async Task<bool> CrawleOneAsync(ProxyManager proxy, string url, SqlManager sql, CancellationToken cancellationToken)
        {
            try
            {
                Uri uriOrig;
                if (!Uri.TryCreate(url, UriKind.Absolute, out uriOrig)) // TOFIX : how this case can happen ???
                {
                    Trace.TraceWarning("CrawleManager.CrawleOneAsync Bad Url Requested : " + url);
                    return false;
                }
                PageEntity page = new PageEntity(uriOrig); //<!> uriOrig may have changed during the normaliz, use only for dnshost
                if (uriOrig.ToString() != page.Url)
                    await sql.FixUri(uriOrig.ToString(), page.Url, cancellationToken);

                string rawHtml;
                try
                {
                    rawHtml = await proxy.DownloadStringTaskAsync(page.HiddenService, page.Url);
                }
                catch (WebException ex)
                {
                    if (!cancellationToken.IsCancellationRequested)
                    {
                        bool isRetry;
                        if (ex.Status != WebExceptionStatus.RequestCanceled)
                        {
                            isRetry = url == page.HiddenService; // default
                            if (ex.Response is HttpWebResponse err)
                            {
                                Trace.TraceInformation("CrawleManager.CrawleOneAsync DownloadStringTaskAsync " + url + " : WebException.HttpWebResponse " + ex.GetBaseException().Message);
                                if (err.StatusCode == HttpStatusCode.Moved || err.StatusCode == HttpStatusCode.MovedPermanently || err.StatusCode == HttpStatusCode.Found)
                                {
                                    string redirectUrl = err.Headers["Location"];
                                    if (!string.IsNullOrEmpty(redirectUrl) && Uri.TryCreate(redirectUrl, UriKind.Absolute, out Uri uri) && UriManager.IsTorUri(uri))
                                    {
                                        redirectUrl = UriManager.NormalizeUrl(uri);
                                        await sql.CrawleRequestEnqueueAsync(redirectUrl, (short)(UriManager.IsHiddenService(redirectUrl) ? 3 : 4), cancellationToken);
                                    }
                                }
                                else if (err.StatusCode == HttpStatusCode.InternalServerError || err.StatusCode == HttpStatusCode.ServiceUnavailable) // servers error are retryed
                                {
                                    isRetry = true;
                                }
                            }
                            else if (ex.Response is SocksHttpWebResponse webResponse)
                            {
                                Trace.TraceInformation("CrawleManager.CrawleOneAsync DownloadStringTaskAsync " + url + " : WebException.SocksHttpWebResponse " + ex.GetBaseException().Message);
                                if (webResponse.StatusCode == HttpStatusCode.Moved || webResponse.StatusCode == HttpStatusCode.MovedPermanently || webResponse.StatusCode == HttpStatusCode.Found)
                                {
                                    string redirectUrl = webResponse.Headers["Location"];
                                    if (!string.IsNullOrEmpty(redirectUrl) && Uri.TryCreate(redirectUrl, UriKind.Absolute, out Uri uri) && UriManager.IsTorUri(uri))
                                    {
                                        redirectUrl = UriManager.NormalizeUrl(uri);
                                        await sql.CrawleRequestEnqueueAsync(redirectUrl, (short)(UriManager.IsHiddenService(redirectUrl) ? 3 : 4), cancellationToken);
                                    }
                                }
                                else if (webResponse.StatusCode == HttpStatusCode.InternalServerError || webResponse.StatusCode == HttpStatusCode.ServiceUnavailable) // servers error are retryed
                                {
                                    isRetry = true;
                                }
                            }
                            else
                            {
                                Trace.TraceInformation("CrawleManager.CrawleOneAsync DownloadStringTaskAsync " + url + " : WebException.?Response " + ex.GetBaseException().Message);
                            }
                        }
                        else //if (ex.Status == WebExceptionStatus.RequestCanceled) // raise by ProxyManager_DownloadProgressChanged
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
                            // default KO management with retry
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
                    Trace.TraceInformation("CrawleManager.CrawleOneAsync DownloadStringTaskAsync " + url + " : Exception " + ex.GetBaseException().ToString());
                    if (!cancellationToken.IsCancellationRequested)
                    {
                        await sql.PageUpdateKo(page, cancellationToken);
#if DEBUG
                        if (Debugger.IsAttached) Debugger.Break();
#endif
                    }
                    return false;
                }

                HtmlDocument htmlDoc = new HtmlDocument();
                try
                {
                    htmlDoc.LoadHtml(rawHtml);
                }
                catch // htmlDoc.LoadHtml May cause a stack overflow on recursive call of HtmlAgilityPack.HtmlNode.CloseNode(HtmlAgilityPack.HtmlNode, Int32), not catcheable by catch Exception
                {
                    Trace.TraceWarning("CrawleManager.CrawleOneAsync LoadHtml " + url + " : Formating Exception");
                    if (!cancellationToken.IsCancellationRequested)
                    {
                        await sql.PageUpdateKo(page, cancellationToken);
#if DEBUG
                        if (Debugger.IsAttached) Debugger.Break();
#endif
                    }
                    return false;
                }
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
                else // some very badly formated site without the <head> for exemple
                {
                    htmlNode2 = htmlDoc.DocumentNode.SelectSingleNode("//*/title"); // look everywhere
                    if (htmlNode2 != null && !string.IsNullOrEmpty(htmlNode2.InnerText))
                        page.Title = PageEntity.NormalizeText(HttpUtility.HtmlDecode(htmlNode2.InnerText));
                    else
                        page.Title = uriOrig.Host;
                }

                // InnerText
                htmlNode2 = htmlDoc.DocumentNode.SelectSingleNode("//body");
                if (htmlNode2 != null && !string.IsNullOrEmpty(htmlNode2.InnerText))
                    page.InnerText = PageEntity.NormalizeText(HttpUtility.HtmlDecode(htmlNode2.InnerText));
                else // some very badly formated site without the <body> for exemple
                {
                    htmlNode2 = htmlDoc.DocumentNode; // will take all the web content
                    if (htmlNode2 != null && !string.IsNullOrEmpty(htmlNode2.InnerText))
                        page.InnerText = PageEntity.NormalizeText(HttpUtility.HtmlDecode(htmlNode2.InnerText));
                    else
                        page.InnerText = string.Empty;  // a null will raise an exception on the sql proc call
                }
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
                    if (htmlNode.Attributes.Contains("href") && !cancellationToken.IsCancellationRequested)
                    {
                        string href = htmlNode.Attributes["href"].Value;

                        Uri uriResult = null;
                        if (href.StartsWith("http"))
                        {
                            if (Uri.TryCreate(href, UriKind.Absolute, out uriResult) && UriManager.IsTorUri(uriResult)) // >=29 because some funny link with just: href='http://'
                            {
                                string str = UriManager.NormalizeUrl(uriResult);
                                if (str.Length < SqlManager.MaxSqlIndex)
                                    if (uriResult.DnsSafeHost != uriOrig.DnsSafeHost)
                                    {
                                        if (!page.OuterLinks.Contains(str) && !page.OuterHdLinks.Contains(str))
                                        {
                                            string hd = UriManager.GetHiddenService(str);    // Digest outter HD
                                            if (hd.Length <= 37 && hd.IndexOfAny(forbiddenInHd) == -1) // some strange url may cause injection
                                            {
                                                if (hd == str)
                                                    page.OuterHdLinks.Add(str);
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
                            if (Uri.TryCreate(uriOrig, href, out uriResult) && UriManager.IsTorUri(uriResult))
                            {
                                string str = UriManager.NormalizeUrl(uriResult);
                                if (str.Length < SqlManager.MaxSqlIndex && !page.InnerLinks.Contains(str))
                                    page.InnerLinks.Add(str);
                            }
                        }
                    }
                htmlDoc = null;
                if (page.InnerLinks.Contains(page.Url)) page.InnerLinks.Remove(page.Url);
                if (page.InnerLinks.Contains(page.HiddenService)) page.InnerLinks.Remove(page.HiddenService);

                await sql.PageInsertOrUpdateOk(page, cancellationToken);
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
                if (Debugger.IsAttached) Debugger.Break();
#endif
                return false; // will retry and not save the page in failed
            }
        }

    }
}
