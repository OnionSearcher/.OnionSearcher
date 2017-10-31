using System;
using System.Collections.Specialized;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Web;

namespace WebSearcherCommon
{
    /// <summary>
    /// Called from the Role, not in the same space than the IIS process, don't mix theses call (not the same BaseDirectory)
    /// </summary>
    public static class UriManager
    {

        private static string[] bannedUrlQuerys = null;
        public static void NormalizeUrlInit(SqlManager sql)
        {
            if (bannedUrlQuerys == null)
                try
                {
                    bannedUrlQuerys = sql.GetBannedUrlQuerys();
                }
                catch (SqlException ex)
                {
                    Trace.TraceWarning("UriManager.NormalizeUrlInit SqlException : " + ex.GetBaseException().Message);
                    bannedUrlQuerys = new string[0];   // non fatal, may continu to works
                }
        }


        public static string NormalizeUrl(Uri absoluteUri)
        {
            if (absoluteUri == null)
                throw new ArgumentNullException("absoluteHref");
            
            for (int i = 0; i < bannedUrlQuerys.Length; i++)
                if (absoluteUri.Query.Contains(bannedUrlQuerys[i]+"="))// may not realy be one searched (thanks to the query "s")
                {
                    bool hasRealyOneQueryMatch = false;
                    NameValueCollection q = HttpUtility.ParseQueryString(absoluteUri.Query);
                    for (int j = i; j < bannedUrlQuerys.Length; j++) // continue since the current loop
                        if (q.AllKeys.Contains(bannedUrlQuerys[j]))
                        {
                            hasRealyOneQueryMatch = true;
                            q.Remove(bannedUrlQuerys[j]);   // case insensitive
                        }
                    if (hasRealyOneQueryMatch)
                    {
                        UriBuilder ub = new UriBuilder(absoluteUri);
                        ub.Query = Uri.EscapeUriString(HttpUtility.UrlDecode(q.ToString())); // q.ToString() force encoding that may don t work on some site (like for the ";" char)
                        absoluteUri = ub.Uri; // will normalise like others Urls
                    }
                    break; // already done for all
                }

            string absoluteHref = absoluteUri.ToString()
                .Replace("&amp;", "&") // it's html encoding, the uri real is not encoded
                .Replace("&shy;", "") // html trick
                .Replace("&#173;", "")
                .Replace("</form>", "");    // bug?

            int iPos = absoluteHref.IndexOf('#');
            if (iPos > 0)
                absoluteHref = absoluteHref.Substring(0, iPos - 1); // remove # in urls

            while (absoluteHref.EndsWith("?", StringComparison.Ordinal)) // remove trailing '?'
                absoluteHref = absoluteHref.Substring(0, absoluteHref.Length - 1);

            if (absoluteHref.IndexOf('/', 8) > 0)
            {
                while (absoluteHref.EndsWith("/", StringComparison.Ordinal) && absoluteHref.IndexOf('/', 8) < (absoluteHref.Length - 1)) // keep the 3 first (include the one after the domain name)
                    absoluteHref = absoluteHref.Substring(0, absoluteHref.Length - 1); // without / at the end, sometime several // can be seen, remove them....
            }
            else // urlStopper may have removed the last / of a domain root
            {
                absoluteHref += '/';
            }

            return absoluteHref;
        }

        public static string GetHiddenService(string url)
        {
            int i = url.IndexOf('/', 29);
            if (i > 0)
                return url.Substring(0, i + 1);
            else
                return url;
        }

        public static bool IsHiddenService(string url)
        {
            int i = url.IndexOf('/', 29);
            if (i > 0)
                return  (i + 1) == url.Length;
            else
                return true;
        }

        public static bool IsTorUri(Uri uri)
        {
            return uri != null
                    && (uri.Scheme == Uri.UriSchemeHttp || uri.Scheme == Uri.UriSchemeHttps)
                    && uri.DnsSafeHost.EndsWith(".onion") && uri.DnsSafeHost.Length == 22;
        }
        
    }
}