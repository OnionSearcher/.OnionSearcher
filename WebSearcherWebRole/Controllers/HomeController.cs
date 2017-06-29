using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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

                    ViewBag.IsFull = Request.QueryString["f"] == "1"; // result grouped by domain or not

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
                                ViewBag.AlertDanger = "Some of your keyword(s) have been removed. Please read the About section for more information on our policy.";
                                q = string.Join(" ", keywords);
                            }

                            ViewBag.Research = q;
                            ViewBag.ResearchUrlEncoded = Url.Encode(q);
                            ViewBag.TitlePrefix = q + " - ";

                            SearchResultEntity ret = sql.GetSearchResult(q, p, ViewBag.IsFull);
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
                        {
                            ViewBag.AlertDanger = "Your keyword(s) have been removed. Please read the About section for more information on our policy. You may use the Contact section if you think there is a mistake.";
                        }
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
                string q = Request.Form["url"];
                if (!string.IsNullOrWhiteSpace(q))
                {
                    DisableCache();

                    q = q.Trim();
                    if (!q.StartsWith("http"))
                    {
                        q = "http://" + q;
                    }

                    if (q.Length < 256 && Uri.TryCreate(q, UriKind.Absolute, out Uri uriResult) && uriResult.IsTor())
                    {
                        StorageManager.EnqueueCrawlerRequest(uriResult.ToString());
                        return Redirect("/?msg=1");
                    }
                    else
                    {
                        ViewBag.AlertDanger = "ERROR : the URL " + q + " doesn't seems to be a valide hidden service, please check and retry.";
                    }
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
                string q = Request.Form["msg"];
                if (!string.IsNullOrWhiteSpace(q))
                {
                    DisableCache();

                    q = q.Trim();

                    if (q.Length < 8000)
                    {
                        StorageManager.EnqueueContact(q);

                        return Redirect("/?msg=2");
                    }
                    else
                    {
                        ViewBag.AlertDanger = "ERROR : The message is to long, make it shorter please.";
                    }
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

        /// <summary>
        /// private_key, server-status
        /// </summary>
        /// <returns></returns>
        public ActionResult Spam()
        {
            Trace.TraceInformation("HomeController.Spam : " + Request.Url);
            return new RedirectResult("http://127.0.0.1", true); // for fun
        }

    }
}