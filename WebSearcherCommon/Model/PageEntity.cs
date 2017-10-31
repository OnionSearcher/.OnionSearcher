using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace WebSearcherCommon
{
    public class PageEntity
    {

        private static readonly Regex regexWhiteSpace = new Regex(@"[\s]{2,}", RegexOptions.None);
        public static string NormalizeText(string innerText)
        {
            if (string.IsNullOrEmpty(innerText))
                throw new ArgumentNullException("innerText");
            return regexWhiteSpace.Replace(
                innerText.Replace("</form>", "")    // may be a bug of HtmlAgilityPack who forget ofen this end tag
                , " ").Trim();
        }
        
        public PageEntity() { }

        public PageEntity(Uri uri)
        {
            Url = UriManager.NormalizeUrl(uri); // normalisation may have change since the poste of the Url in the queue
            HiddenService = UriManager.GetHiddenService(Url);
            LastCrawle = DateTime.UtcNow;
        }

        /// <summary>
        /// Max lenght 37
        /// </summary>
        public string HiddenService { get; set; }
        public string Url { get; set; }

        public string Title { get; set; }
        public string InnerText { get; set; }
        public string Heading { get; set; }

        public HashSet<string> InnerLinks { get; set; }
        public HashSet<string> OuterLinks { get; set; }
        public HashSet<string> OuterHdLinks { get; set; }

        /// <summary>
        /// UTC Time
        /// </summary>
        public DateTime LastCrawle { get; set; }

        public override string ToString()
        {
            return Url;
        }

    }
}
