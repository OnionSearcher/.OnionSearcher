using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Text.RegularExpressions;

namespace WebSearcherCommon
{
    public class PageEntity
    {

        private static readonly StringCollection urlStopper = Settings.Default.UrlStopper; // local cache for faster access

        private static readonly Regex regexWhiteSpace = new Regex(@"[\s]{2,}", RegexOptions.None);

        public static string NormalizeUrl(string absoluteHref)
        {
            if (string.IsNullOrEmpty(absoluteHref))
                throw new ArgumentNullException("absoluteHref");

            absoluteHref = absoluteHref.Replace("&shy;", "").Replace("&#173;", "").Replace("</form>", "");

            int iPos = absoluteHref.IndexOf('#');
            if (iPos > 0)
                absoluteHref = absoluteHref.Substring(0, iPos - 1); // remove # in urls

            // check if a match in Settings.Default.UrlMatchNotIndexed
            foreach (string str in urlStopper)
            {
                int i = absoluteHref.IndexOf(str);
                if (i > 0)
                    absoluteHref = absoluteHref.Substring(0, i - 1);
            }

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
        
        public static string NormalizeText(string innerText)
        {
            if (string.IsNullOrEmpty(innerText))
                throw new ArgumentNullException("innerText");
            return regexWhiteSpace.Replace(innerText, " ").Trim();
        }

        public static string GetHiddenService(string url)
        {
            int i = url.IndexOf('/', 29);
            if (i > 0)
                return url.Substring(0, i + 1);
            else
                return url;
        }

        public PageEntity() { }

        public PageEntity(Uri uri)
        {
            Url = uri.ToString();
            HiddenService = GetHiddenService(Url);
            LastCrawle = DateTime.UtcNow;
        }

        /// <summary>
        /// Max lenght 37
        /// </summary>
        public string HiddenService { get; set; }
        public string Url { get; set; }

#if SAVEHTMLRAW
        public string HtmlRaw { get; set; }
#endif
        public string Title { get; set; }
        public string InnerText { get; set; }
        public string Heading { get; set; }
        
        public HashSet<string> OuterHdLinks { get; set; }

        public DateTimeOffset FirstCrawle { get; set; }
        public DateTimeOffset LastCrawle { get; set; }

        public short? CrawleError { get; set; }

        public override string ToString()
        {
            return Url;
        }

    }
}
