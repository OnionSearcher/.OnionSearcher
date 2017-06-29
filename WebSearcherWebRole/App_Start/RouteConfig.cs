using System.Web.Mvc;
using System.Web.Routing;

namespace WebSearcherWebRole
{
    public class RouteConfig
    {
        public static void RegisterRoutes(RouteCollection routes)
        {
            // Spam... (could also add some blank file to avoid theses errors in log and CPU usage)
            routes.MapRoute(
                name: "spam1",
                url: "server-status",
                defaults: new { controller = "Home", action = "Spam" },
                namespaces: new[] { "WebSearcherWebRole" }
            ).DataTokens["UseNamespaceFallback"] = false;
            routes.MapRoute(
                name: "spam2",
                url: "private_key",
                defaults: new { controller = "Home", action = "Spam" },
                namespaces: new[] { "WebSearcherWebRole" }
            ).DataTokens["UseNamespaceFallback"] = false;

            routes.MapRoute(
                name: "Default",
                url: "{action}",
                defaults: new { controller = "Home", action = "Index" },
                namespaces: new[] { "WebSearcherWebRole" }
            ).DataTokens["UseNamespaceFallback"] = false;

        }

    }
}
