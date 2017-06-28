
namespace WebSearcherCommon
{
    public class PageResultEntity
    {

        public PageResultEntity() { }

        public string UrlClick { get; set; }
        public string Url { get; set; }
        public string UrlToolTip { get; set; }

        public string Title { get; set; }
        public string TitleToolTip { get; set; }
        
        public string InnerText { get; set; }
        
        public bool CrawleError { get; set; }
        
        public int DaySinceLastCrawle { get; set; }
        public int HourSinceLastCrawle { get; set; }
        
        public string HiddenServiceMain { get; set; }
        public string HiddenServiceMainClick { get; set; }
        
        public override string ToString()
        {
            return Url;
        }

    }
}
