using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Web;
using System.Web.Mvc;
using System.Web.UI;
using WebSearcherCommon;

namespace WebSearcherWebRole
{

#if DEBUG
    [OutputCache(Duration = 5, Location = OutputCacheLocation.Client, VaryByParam = "*")] // JUST TO keep the same kind of header in debug env
#else
    [OutputCache(Duration = 604800, Location = OutputCacheLocation.Any, VaryByParam = "*")] // 7 days
#endif
    public class HomeController : Controller // for the search part
    {

        private static DateTime _StopWordsDate = DateTime.MinValue;
        private static HashSet<string> _StopWords;
        private static HashSet<string> GetStopWords(SqlManager sql)
        {
            if (_StopWordsDate < DateTime.Now)
            {
                _StopWords = null;
            }
            if (_StopWords == null)
            {
                _StopWords = sql.GetStopWords();
                _StopWordsDate = DateTime.Now.Add(Settings.Default.StopWordsRefresh);
            }
            return _StopWords;
        }

        internal void ShorterCache()
        {
#if DEBUG
            HttpContext.Response.Cache.SetMaxAge(TimeSpan.FromMinutes(0.01));
            HttpContext.Response.Cache.SetExpires(DateTime.UtcNow.AddMinutes(0.01));
#else
            HttpContext.Response.Cache.SetMaxAge(TimeSpan.FromMinutes(60.0));
            HttpContext.Response.Cache.SetExpires(DateTime.UtcNow.AddMinutes(60.0));
            
#endif
        }
        internal void DisableCache()
        {
            HttpContext.Response.Cache.SetNoStore();
            HttpContext.Response.Cache.SetCacheability(HttpCacheability.NoCache);
        }

        private static readonly char[] splitChar = new char[] { ' ' };

        public ActionResult Index()
        {
            Trace.TraceInformation("HomeController.Index : " + Request.RawUrl);

            //ViewBag.AlertDanger = "DATABASE MAINTENANCE, please come back tomorrow.";

            if (Request.QueryString.Count == 0)
            {
                ViewBag.FastPageDisplay = true;
            }
            else
            {
                string q = Request.QueryString["q"];
                if (!string.IsNullOrWhiteSpace(q))
                {
                    short p = 1;
                    if (!string.IsNullOrWhiteSpace(Request.QueryString["p"]))
                    {
                        if (short.TryParse(Request.QueryString["p"], out p))
                        {
                            if (p <= 0)
                                p = 1;
                        }
                    }

                    ShorterCache();
                    using (SqlManager sql = new SqlManager())
                    {
                        HashSet<string> sw = GetStopWords(sql);
                        bool hasSW = false;
                        List<string> keywords = q.Split(splitChar).ToList();
                        for (int i = keywords.Count - 1; i >= 0; i--)
                            if (sw.Contains(keywords[i].ToLowerInvariant()))
                            {
                                hasSW = true;
                                keywords.RemoveAt(i);
                            }

                        if (keywords.Count > 0)
                        {
                            if (hasSW)
                            {
                                ViewBag.AlertWarning = "Some of your keyword(s) have been removed. Please read the About section for more information on usage and policy.";
                                q = string.Join(" ", keywords);
                            }

                            ViewBag.Research = q;
                            ViewBag.ResearchUrlEncoded = Url.Encode(q);
                            ViewBag.TitlePrefix = q + " - ";
                            ViewBag.IsFull = Request.QueryString["f"] == "1"; // result grouped by domain or not

                            SearchResultEntity ret;
                            // Advanced search?
                            if (!keywords[0].Contains(':'))
                                ret = sql.GetSearchResult(q, p, ViewBag.IsFull);
                            else
                            {
                                Uri url;
                                if (keywords[0].StartsWith("site:", StringComparison.InvariantCultureIgnoreCase) && keywords[0].Length > 5)
                                {
                                    ViewBag.IsFull = null; // removing filtering IHM
                                    if (Uri.TryCreate((!keywords[0].StartsWith("site:http:") ? "http://" : "") + keywords[0].Substring(5), UriKind.Absolute, out url) && PageEntity.IsTorUri(url))
                                        ret = sql.GetSearchSiteResult(PageEntity.GetHiddenService(url.ToString()), p);
                                    else
                                    {
                                        ViewBag.AlertDanger = "Error processing the Url. Please check or contact us.";
                                        return View();
                                    }
                                }
                                else if (keywords[0].StartsWith("cache:", StringComparison.InvariantCultureIgnoreCase) && keywords[0].Length > 6)
                                {
                                    ViewBag.IsFull = null; // removing filtering IHM
                                    if (Uri.TryCreate((!keywords[0].StartsWith("cache:http:") ? "http://" : "") + keywords[0].Substring(6), UriKind.Absolute, out url) && PageEntity.IsTorUri(url))
                                        ret = sql.GetSearchCachedResult(url.ToString());
                                    else
                                    {
                                        ViewBag.AlertDanger = "Error processing the Url. Please check or contact us.";
                                        return View();
                                    }
                                }
                                else if (q.StartsWith("intitle:", StringComparison.InvariantCultureIgnoreCase) && q.Length > 8)
                                    ret = sql.GetSearchTitleResult(q.Substring(8), p, ViewBag.IsFull);
                                else if (q.StartsWith("allintitle:", StringComparison.InvariantCultureIgnoreCase) && q.Length > 11)
                                    ret = sql.GetSearchTitleResult(q.Substring(11), p, ViewBag.IsFull);
                                else if (q.StartsWith("intext:", StringComparison.InvariantCultureIgnoreCase) && q.Length > 7)
                                    ret = sql.GetSearchInnerTextResult(q.Substring(7), p, ViewBag.IsFull);
                                else if (q.StartsWith("allintext:", StringComparison.InvariantCultureIgnoreCase) && q.Length > 10)
                                    ret = sql.GetSearchInnerTextResult(q.Substring(10), p, ViewBag.IsFull);
                                else // normal search with a ":"
                                    ret = sql.GetSearchResult(q, p, ViewBag.IsFull);
                            }

                            ViewBag.Results = ret.Results;
                            ViewBag.ResultsTotalNb = ret.ResultsTotalNb;
                            ViewBag.Page = p;
                            if (ret.ResultsTotalNb > 10)
                            {
                                ViewBag.Previous = true;
                                ViewBag.Next = true;
                                if (p < 4)
                                {
                                    ViewBag.Previous = (p != 1);
                                    ViewBag.PageA = 1;
                                    ViewBag.PageB = 2;
                                    ViewBag.PageC = 3;
                                    ViewBag.PageD = 4;
                                    ViewBag.PageE = 5;
                                }
                                else if ((p + 1) * 10 < ret.ResultsTotalNb)
                                {
                                    ViewBag.PageA = p - 2;
                                    ViewBag.PageB = p - 1;
                                    ViewBag.PageC = p;
                                    ViewBag.PageD = p + 1;
                                    ViewBag.PageE = p + 2;
                                }
                                else if (p * 10 < ret.ResultsTotalNb)
                                {
                                    ViewBag.PageA = p - 3;
                                    ViewBag.PageB = p - 2;
                                    ViewBag.PageC = p - 1;
                                    ViewBag.PageD = p;
                                    ViewBag.PageE = p + 1;
                                }
                                else
                                {
                                    ViewBag.PageA = p - 4;
                                    ViewBag.PageB = p - 3;
                                    ViewBag.PageC = p - 2;
                                    ViewBag.PageD = p - 1;
                                    ViewBag.PageE = p;
                                    ViewBag.Next = false;
                                }
                            }
                            return View("Result");
                        }
                        else
                            ViewBag.AlertDanger = "Your keyword(s) have been removed. Please read the About section for more information on usage and policy. You may use the Contact section if you think there is a mistake.";
                    }
                }
                else
                {
                    q = Request.QueryString["url"];
                    if (!string.IsNullOrWhiteSpace(q))
                    {
                        ShorterCache();
                        return Redirect(q);
                    }
                    else
                    {
                        // normal cache
                        q = Request.QueryString["msg"];
                        if (!string.IsNullOrWhiteSpace(q))
                        {
                            switch (q)
                            {
                                case "1":
                                    ViewBag.AlertSuccess = "Thanks, the URL will be checked soon.";
                                    break;
                                case "2":
                                    ViewBag.AlertSuccess = "Thanks for contacting us.";
                                    break;
                            }
                        }
                    }
                }
            }
            return View();
        }

        public ActionResult Result()
        {
            Trace.TraceInformation("HomeController.Result : " + Request.RawUrl);
            throw new NotImplementedException(); // managed by Index() in fact, just here to please MVC pattern
        }

        public ActionResult Add()
        {
            Trace.TraceInformation("HomeController.Add : " + Request.RawUrl);
            ViewBag.TitlePrefix = "Add a hidden service to ";

            if (Request.Form.Count > 0)
            {
                string url = Request.Form["url"];
                if (!string.IsNullOrWhiteSpace(url))
                {
                    DisableCache();

                    url = url.Trim();
                    if (!url.StartsWith("http"))
                        url = "http://" + url;

                    if (url.Length < SqlManager.MaxSqlIndex && Uri.TryCreate(url, UriKind.Absolute, out Uri uriResult) && PageEntity.IsTorUri(uriResult))
                    {
                        url = PageEntity.NormalizeUrl(url);
                        using (SqlManager sql = new SqlManager())
                        {
                            sql.CrawleRequestEnqueue(url);
                        }
                    }

                    return Redirect("/?msg=1");
                }
                else
                {
                    ViewBag.AlertDanger = "ERROR : the URL " + url + " doesn't seems to be a valide hidden service, please check and retry.";
                }
            }

            return View();
        }

        public ActionResult Contact()
        {
            Trace.TraceInformation("HomeController.Contact : " + Request.RawUrl);
            ViewBag.TitlePrefix = "Contact ";

            if (Request.Form.Count > 0)
            {
                DisableCache();
                string msg = Request.Form["msg"];
                if (!string.IsNullOrWhiteSpace(msg))
                {
                    msg = msg.Trim();
                    if (msg.Length <= 4000)
                    {
                        using (SqlManager sql = new SqlManager())
                        {
                            sql.PostContactMessage(msg);
                        }
                        return Redirect("/?msg=2");
                    }
                    else
                    {
                        ViewBag.AlertDanger = "ERROR : The message is to long, make it shorter please.";
                    }
                }
                else
                {
                    ViewBag.AlertDanger = "ERROR : The message is empty.";
                }
            }

            return View();
        }

        public ActionResult About()
        {
            Trace.TraceInformation("HomeController.About : " + Request.RawUrl);
            ViewBag.TitlePrefix = "About ";
            
            return View();
        }
        

        public ActionResult Error()
        {
            Trace.TraceInformation("HomeController.Error : " + Request.Url);

            HttpStatusCode ret;
            if (!Enum.TryParse(Request.QueryString.ToString(), out ret))
            {
                ret = HttpStatusCode.Forbidden;
            }

            Response.TrySkipIisCustomErrors = true;
            Response.StatusCode = (int)ret;
            ViewBag.TitlePrefix = "ERROR - ";
            ViewBag.AlertDanger = "ERROR " + (int)ret + " : " + ret;

            return View();
        }

        [ActionNames("server-status", "private_key")]
        public ActionResult SpamAction()
        {
            Trace.TraceInformation("HomeController.SpamAction : " + Request.Url);
            return new RedirectResult("http://127.0.0.1", true); // for fun
        }

    }
}