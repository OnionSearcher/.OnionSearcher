# .OnionSearcher
(dot)Onion Searcher

An open source, anonymous, no-script friendly, ads free, search engine dedicated to the Tor network.
Advanced search function : cache: site: intitle: intext:

## TOFIX

- A lot !
- Search results quality
- Ranking quality
- UTF16 pages don't display correctly
- Trace don't work for WebRole class (but the reel web trace are OK, juste the Azure service himself
- Fix CounterRoleStarted.Increment
- Anonymous Tor process
- Crawlers stability
- WebException  Bad Request
- restart new url scan on old hd and page to fix (no gode right and need a proc)
- ban redirection on normal web
- ssl error "A call to SSPI failed" / "The function requested is not supported"
- Manage redirect like a new Url
- WebException  Bad Gateway

## TODO

- Url cleaner (sid remover for exemple)
- Improve full text search
- Better english
- Abuse/Report system for user
- Support site: and links: inurl: link:  allinurl:  inurl:
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
