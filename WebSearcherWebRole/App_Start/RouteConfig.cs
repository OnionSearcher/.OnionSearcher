using System.Web.Mvc;
using System.Web.Routing;

namespace WebSearcherWebRole
{
    public class RouteConfig
    {
        public static void RegisterRoutes(RouteCollection routes)
        {
            //routes.IgnoreRoute("{resource}.axd/{*pathInfo}");

            routes.MapRoute(
                name: "Default",
                url: "{action}/{value}",
                defaults: new { controller = "Home", action = "Index", value = UrlParameter.Optional }
            );

        }
    }
}
