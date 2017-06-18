using System;
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

        internal void ShorterCache()
        {
#if !DEBUG
            this.HttpContext.Response.Cache.SetMaxAge(TimeSpan.FromMinutes(15.0));
            this.HttpContext.Response.Cache.SetExpires(DateTime.UtcNow.AddMinutes(15.0));
#endif
        }
        internal void DisableCache()
        {
            this.HttpContext.Response.Cache.SetNoStore();
            this.HttpContext.Response.Cache.SetCacheability(HttpCacheability.NoCache);
        }

        public ActionResult Index()
        {
            if (Request.QueryString.Count == 0)
            {
                ViewBag.FastPageDisplay = true;
            }
            else
            {
                string q = Request.QueryString["q"];
                if (!string.IsNullOrWhiteSpace(q))
                {
                    ViewBag.Research = q;
                    ViewBag.ResearchUrlEncoded = Url.Encode(q);
                    ViewBag.TitlePrefix = q + " - ";

                    StorageManager.EnqueueHistoSearch(q);

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
                }
                else
                {
                    q = Request.QueryString["url"];
                    if (!string.IsNullOrWhiteSpace(q))
                    {
                        StorageManager.EnqueueHistoClick(q);

                        DisableCache();
                        return Redirect(q);
                    }
                    else
                    {
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
            throw new NotImplementedException(); // managed by Index() in fact, just here to please MVC pattern
        }

        public ActionResult Add()
        {
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
            ViewBag.TitlePrefix = "About ";

            return View();
        }

    }
}