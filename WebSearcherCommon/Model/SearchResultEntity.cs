using System.Collections;
using System.Collections.Generic;

namespace WebSearcherCommon
{
    public class SearchResultEntity
    {

        public SearchResultEntity() { }

        public int ResultsTotalNb { get; set; }

        public IList<PageResultEntity> Results { get; set; }

    }
}
