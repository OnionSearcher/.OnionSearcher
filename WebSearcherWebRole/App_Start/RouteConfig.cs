using System.Web.Mvc;
using System.Web.Routing;

namespace WebSearcherWebRole
{
    public class RouteConfig
    {
        public static void RegisterRoutes(RouteCollection routes)
        {
            routes.MapRoute(
                name: "Default",
                url: "{action}",
                defaults: new { controller = "Home", action = "Index" },
                namespaces: new[] { "WebSearcherWebRole" }
            ).DataTokens["UseNamespaceFallback"] = false;

        }

    }
}
