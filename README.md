# .OnionSearcher
(dot)Onion Searcher

An open source, anonymous, no-script friendly, ads free, search engine dedicated to the Tor network.
Advanced search function : cache: site: intitle: intext: inurl:

## TOFIX

- A lot !
- Search results quality
- Ranking quality
- UTF16 pages doesn't always display correctly
- restart new url scan on old hd and page to fix (no gode right and need a proc)
- ssl error "A call to SSPI failed" / "The function requested is not supported"
- Url rencoding when removing some query forece urlencode that may break the page like this force encoding of some param not required like ; and web server may not interpret them correctly, like http://37327zww2mdb76ie.onion/?p=.git;a=history;f=test;h=e69de29bb2d1d6434b8b29ae775ad8c2e48c5391;hb=HEAD

## TODO

- Url cleaner (sid remover for exemple)
- Improve full text search
- Better english
- Abuse/Report system direct from result
- Filter HTTP200 page with HTTP Error text only

## Contributing

Requirement :

- Visual Studio Community 2017, with at last theses modules
    - ASP.Net and web development
    - Azure developement
    - Data storage and processing
- Have an SQL Server database
- Download Tor Expert Bundle https://www.torproject.org/download/download.html.en to the folder WebSearcherCommon\ExpertBundle
- Download the lib https://github.com/OnionSearcher/BetterHttpClient.git to the folder ..\BetterHttpClient

Whishlist :

- Get contributions !
- Get styled !
- Non english display support
- Replace SQL Server and his full text search by Elasticsearch or better
- Replace .Net 4.6 by .Net Core 2 when it will be released
- Upgrade to Bootstrap 4 when it will be released
