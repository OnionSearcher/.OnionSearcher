CREATE OR ALTER FUNCTION TrimTextSearch(@Keywords NVARCHAR(64), @InnerText NVARCHAR(MAX)) RETURNS NVARCHAR(250) AS
BEGIN
	DECLARE @MaxLenght INT = 200
	DECLARE @LeftWords INT = 30
	DECLARE @RightWords INT = 20
	DECLARE @ret NVARCHAR(250) -- @MaxLenght+@LeftWords+@RightWords

	DECLARE @i INT
	SET @i=COALESCE((SELECT MIN(CHARINDEX(value,@InnerText)) FROM STRING_SPLIT(@Keywords,' ') WHERE CHARINDEX (value,@InnerText)>0),0)
	IF(@i>@LeftWords)
	BEGIN
		SET @ret=SUBSTRING(@InnerText,@i-@LeftWords, @MaxLenght+@LeftWords+@RightWords) --get a litle more for trim safety
		SET @ret=SUBSTRING(@ret,CHARINDEX(' ',@ret)+1, @MaxLenght+@RightWords)
	END
	ELSE
		SET @ret=SUBSTRING(@InnerText,0,@MaxLenght+@RightWords) -- get a litle more for trim safety
		
	SET @i=CHARINDEX(' ',@ret,@MaxLenght-@RightWords)
	IF(@i>0)
		SET @ret= SUBSTRING(@ret,0,@i) -- clean trim
	ELSE
		SET @ret= SUBSTRING(@ret,0,@MaxLenght)+'...' -- cut in a word
	RETURN @ret
END
GO

CREATE OR ALTER PROCEDURE Search2(@Keywords NVARCHAR(64), @Page SMALLINT=0, @Full SMALLINT=0) AS  -- have to set a length for @Keywords else bugous results
BEGIN
    SET NOCOUNT ON
	DECLARE @FreeTextTable_Weighting SMALLINT = 500 -- FREETEXTTABLE from 0 to 1000
	DECLARE @ResultPerPage INT = 10 -- FREETEXTTABLE from 0 to 1000 , set as 0 to 2 versus the 0 to 1 of the Page.Rank
	
	DECLARE @UrlsToGetTenResults INT
	SET @UrlsToGetTenResults = 1000 + 1000 * @Page  -- random value in order to expect just 10 final result for first page !
	
	DECLARE @MaxResult INT
	SELECT @MaxResult=COUNT(1) FROM FREETEXTTABLE(Pages, *, @Keywords) r
	SELECT @MaxResult -- returned no called

	IF (@Full=0 AND @MaxResult>10) -- the @@ROWCOUNT>10 should be improved because then low result, the group by may be too string
	BEGIN
		SET @MaxResult = CASE WHEN @MaxResult>=@UrlsToGetTenResults THEN 1 WHEN @MaxResult>(100+100*@Page) THEN 2 ELSE 3 END --

		SELECT p2.Url, COALESCE(Title, p2.HiddenService) Title, dbo.TrimTextSearch(@Keywords, InnerText) as InnerText, CrawleError
				,DATEDIFF(day, LastCrawle,SYSUTCDATETIME()) DaySinceLastCrawle, DATEDIFF(hour, LastCrawle,SYSUTCDATETIME()) HourSinceLastCrawle
				,m.HiddenServiceMain
			FROM (
				SELECT  Url, r.RANK+@FREETEXTTABLE_Weighting*p.Rank r, ROW_NUMBER() OVER(PARTITION BY HiddenService ORDER BY (r.RANK+@FREETEXTTABLE_Weighting*p.Rank) DESC) n
					FROM ( SELECT s.[KEY], SUM(s.[RANK]) [RANK] FROM (
						SELECT * FROM FREETEXTTABLE(Pages, (Title, Heading) , @Keywords, LANGUAGE 1033, @UrlsToGetTenResults)
						UNION ALL SELECT [KEY], [RANK]*0.5 FROM FREETEXTTABLE(Pages, InnerText , @Keywords, LANGUAGE 1033, @UrlsToGetTenResults)
					) s GROUP BY [KEY] ) r
					INNER JOIN Pages p WITH (NOLOCK) ON p.Url=r.[KEY]
				) s
				INNER JOIN Pages p2 WITH (NOLOCK) ON s.Url=p2.Url
				LEFT JOIN HiddenServiceMirrors m WITH (NOLOCK) ON m.HiddenService=p2.HiddenService
			WHERE s.n<=@MaxResult -- may help a little when low result
			ORDER BY s.r DESC
			OFFSET @Page*@ResultPerPage ROWS FETCH NEXT @ResultPerPage ROWS ONLY

	END
	ELSE IF (@MaxResult>0)

		SELECT Url, COALESCE(Title, p.HiddenService) Title, dbo.TrimTextSearch(@Keywords, InnerText) as InnerText, CrawleError
					,DATEDIFF(day, LastCrawle,SYSUTCDATETIME()) DaySinceLastCrawle, DATEDIFF(hour, LastCrawle,SYSUTCDATETIME()) HourSinceLastCrawle
					,m.HiddenServiceMain
				FROM ( SELECT s.[KEY], SUM(s.[RANK]) [RANK] FROM (
						SELECT * FROM FREETEXTTABLE(Pages, (Title, Heading) , @Keywords, LANGUAGE 1033, @UrlsToGetTenResults)
						UNION ALL SELECT [KEY], [RANK]*0.5 FROM FREETEXTTABLE(Pages, InnerText , @Keywords, LANGUAGE 1033, @UrlsToGetTenResults)
					) s GROUP BY [KEY] ) r
					INNER JOIN Pages p WITH (NOLOCK) ON p.Url=r.[KEY]
					LEFT JOIN HiddenServiceMirrors m WITH (NOLOCK) ON m.HiddenService=p.HiddenService
				ORDER BY r.RANK+@FREETEXTTABLE_Weighting*p.Rank DESC
				OFFSET @Page*@ResultPerPage ROWS FETCH NEXT @ResultPerPage ROWS ONLY
END
GO
GRANT EXECUTE ON Search2 TO sqlReader
GO

CREATE OR 
ALTER   PROCEDURE [dbo].[SearchTitle](@Keywords NVARCHAR(64), @Page SMALLINT=0, @Full SMALLINT=0) AS  -- have to set a length for @Keywords else bugous results
BEGIN
    SET NOCOUNT ON
	DECLARE @FreeTextTable_Weighting SMALLINT = 500 -- FREETEXTTABLE from 0 to 1000
	DECLARE @ResultPerPage INT = 10 -- FREETEXTTABLE from 0 to 1000 , set as 0 to 2 versus the 0 to 1 of the Page.Rank
	
	DECLARE @UrlsToGetTenResults INT
	SET @UrlsToGetTenResults = 1000 + 1000 * @Page  -- random value in order to expect just 10 final result for first page !
	
	DECLARE @MaxResult INT
	SELECT @MaxResult=COUNT(1) FROM FREETEXTTABLE(Pages, Title, @Keywords) r
	SELECT @MaxResult -- returned no called

	IF (@Full=0 AND @MaxResult>10) -- the @@ROWCOUNT>10 should be improved because then low result, the group by may be too string
	BEGIN
		SET @MaxResult = CASE WHEN @MaxResult>=@UrlsToGetTenResults THEN 1 WHEN @MaxResult>(100+100*@Page) THEN 2 ELSE 3 END --

		SELECT p2.Url, COALESCE(Title, p2.HiddenService) Title, dbo.TrimTextSearch(@Keywords, InnerText) as InnerText, CrawleError
				,DATEDIFF(day, LastCrawle,SYSUTCDATETIME()) DaySinceLastCrawle, DATEDIFF(hour, LastCrawle,SYSUTCDATETIME()) HourSinceLastCrawle
				,m.HiddenServiceMain
			FROM (
				SELECT  Url, r.RANK+@FREETEXTTABLE_Weighting*p.Rank r, ROW_NUMBER() OVER(PARTITION BY HiddenService ORDER BY (r.RANK+@FREETEXTTABLE_Weighting*p.Rank) DESC) n
					FROM FREETEXTTABLE(Pages, Title, @Keywords, LANGUAGE 1033, @UrlsToGetTenResults) r
					INNER JOIN Pages p WITH (NOLOCK) ON p.Url=r.[KEY]
				) s
				INNER JOIN Pages p2 WITH (NOLOCK) ON p2.url=s.url
				LEFT JOIN HiddenServiceMirrors m WITH (NOLOCK) ON m.HiddenService=p2.HiddenService
			WHERE s.n<=@MaxResult -- may help a little when low result
			ORDER BY s.r DESC
			OFFSET @Page*@ResultPerPage ROWS FETCH NEXT @ResultPerPage ROWS ONLY

	END
	ELSE IF (@MaxResult>0)

		SELECT Url, COALESCE(Title, p.HiddenService) Title, dbo.TrimTextSearch(@Keywords, InnerText) as InnerText, CrawleError
					,DATEDIFF(day, LastCrawle,SYSUTCDATETIME()) DaySinceLastCrawle, DATEDIFF(hour, LastCrawle,SYSUTCDATETIME()) HourSinceLastCrawle
					,m.HiddenServiceMain
				FROM FREETEXTTABLE(Pages, Title, @Keywords, LANGUAGE 1033, @UrlsToGetTenResults) r
					INNER JOIN Pages p WITH (NOLOCK) ON p.Url=r.[KEY]
					LEFT JOIN HiddenServiceMirrors m WITH (NOLOCK) ON m.HiddenService=p.HiddenService
				ORDER BY r.RANK+@FREETEXTTABLE_Weighting*p.Rank DESC
				OFFSET @Page*@ResultPerPage ROWS FETCH NEXT @ResultPerPage ROWS ONLY

END
GO
GRANT EXECUTE ON SearchTitle TO sqlReader
GO

CREATE OR ALTER   PROCEDURE [dbo].[SearchInnerText](@Keywords NVARCHAR(64), @Page SMALLINT=0, @Full SMALLINT=0) AS  -- have to set a length for @Keywords else bugous results
BEGIN
    SET NOCOUNT ON
	DECLARE @FreeTextTable_Weighting SMALLINT = 500 -- FREETEXTTABLE from 0 to 1000
	DECLARE @ResultPerPage INT = 10 -- FREETEXTTABLE from 0 to 1000 , set as 0 to 2 versus the 0 to 1 of the Page.Rank
	
	DECLARE @UrlsToGetTenResults INT
	SET @UrlsToGetTenResults = 1000 + 1000 * @Page  -- random value in order to expect just 10 final result for first page !
	
	DECLARE @MaxResult INT
	SELECT @MaxResult=COUNT(1) FROM FREETEXTTABLE(Pages, InnerText, @Keywords) r
	SELECT @MaxResult -- returned no called

	IF (@Full=0 AND @MaxResult>10) -- the @@ROWCOUNT>10 should be improved because then low result, the group by may be too string
	BEGIN
		SET @MaxResult = CASE WHEN @MaxResult>=@UrlsToGetTenResults THEN 1 WHEN @MaxResult>(100+100*@Page) THEN 2 ELSE 3 END --

		SELECT p2.Url, COALESCE(Title, p2.HiddenService) Title, dbo.TrimTextSearch(@Keywords, InnerText) as InnerText, CrawleError
				,DATEDIFF(day, LastCrawle,SYSUTCDATETIME()) DaySinceLastCrawle, DATEDIFF(hour, LastCrawle,SYSUTCDATETIME()) HourSinceLastCrawle
				,m.HiddenServiceMain
			FROM (
				SELECT  Url, HiddenService, r.RANK+@FREETEXTTABLE_Weighting*p.Rank r, ROW_NUMBER() OVER(PARTITION BY HiddenService ORDER BY (r.RANK+@FREETEXTTABLE_Weighting*p.Rank) DESC) n
					FROM FREETEXTTABLE(Pages, InnerText, @Keywords, LANGUAGE 1033, @UrlsToGetTenResults) r
					INNER JOIN Pages p WITH (NOLOCK) ON p.Url=r.[KEY]
				) s
				INNER JOIN Pages p2 WITH (NOLOCK) ON s.Url=p2.Url
				LEFT JOIN HiddenServiceMirrors m WITH (NOLOCK) ON m.HiddenService=p2.HiddenService
			WHERE s.n<=@MaxResult -- may help a little when low result
			ORDER BY s.r DESC
			OFFSET @Page*@ResultPerPage ROWS FETCH NEXT @ResultPerPage ROWS ONLY

	END
	ELSE IF (@MaxResult>0)

		SELECT Url, COALESCE(Title, p.HiddenService) Title, dbo.TrimTextSearch(@Keywords, InnerText) as InnerText, CrawleError
					,DATEDIFF(day, LastCrawle,SYSUTCDATETIME()) DaySinceLastCrawle, DATEDIFF(hour, LastCrawle,SYSUTCDATETIME()) HourSinceLastCrawle
					,m.HiddenServiceMain
				FROM FREETEXTTABLE(Pages, InnerText, @Keywords, LANGUAGE 1033, @UrlsToGetTenResults) r
					INNER JOIN Pages p WITH (NOLOCK) ON p.Url=r.[KEY]
					LEFT JOIN HiddenServiceMirrors m WITH (NOLOCK) ON m.HiddenService=p.HiddenService
				ORDER BY r.RANK+@FREETEXTTABLE_Weighting*p.Rank DESC
				OFFSET @Page*@ResultPerPage ROWS FETCH NEXT @ResultPerPage ROWS ONLY

END
GO
GRANT EXECUTE ON SearchInnerText TO sqlReader
GO

CREATE OR 
ALTER   PROCEDURE [dbo].[SearchUrl](@Keywords NVARCHAR(64), @Page SMALLINT=0, @Full SMALLINT=0) AS
BEGIN
    SET NOCOUNT ON
	DECLARE @ResultPerPage INT = 10 -- FREETEXTTABLE from 0 to 1000 , set as 0 to 2 versus the 0 to 1 of the Page.Rank
	
	DECLARE @UrlsToGetTenResults INT
	SET @UrlsToGetTenResults = 100 + 100 * @Page  -- random value in order to expect just 10 final result for first page !
	
	DECLARE @MaxResult INT
	SELECT @MaxResult=COUNT(1) FROM Pages WITH (NOLOCK) WHERE Url LIKE '%'+@Keywords+'%'
	SELECT @MaxResult -- returned no called

	IF (@Full=0 AND @MaxResult>10) -- the @@ROWCOUNT>10 should be improved because then low result, the group by may be too string
	BEGIN
		SET @MaxResult = CASE WHEN @MaxResult>@UrlsToGetTenResults THEN 1 WHEN @MaxResult>(10+10*@Page) THEN 2 ELSE 3 END --

		SELECT p2.Url, COALESCE(Title, p2.HiddenService) Title, Heading as InnerText, CrawleError
				,DATEDIFF(day, LastCrawle,SYSUTCDATETIME()) DaySinceLastCrawle, DATEDIFF(hour, LastCrawle,SYSUTCDATETIME()) HourSinceLastCrawle
				,m.HiddenServiceMain
			FROM (
				SELECT  Url, p.Rank r, ROW_NUMBER() OVER(PARTITION BY HiddenService ORDER BY (p.Rank) DESC) n
					FROM Pages p WITH (NOLOCK)
					WHERE p.Url LIKE '%'+@Keywords+'%'
				) s
				INNER JOIN Pages p2 WITH (NOLOCK) ON p2.url=s.url
				LEFT JOIN HiddenServiceMirrors m WITH (NOLOCK) ON m.HiddenService=p2.HiddenService
			WHERE s.n<=@MaxResult -- may help a little when low result
			ORDER BY s.r DESC
			OFFSET @Page*@ResultPerPage ROWS FETCH NEXT @ResultPerPage ROWS ONLY

	END
	ELSE IF (@MaxResult>0)

		SELECT Url, COALESCE(Title, p.HiddenService) Title, Heading as InnerText, CrawleError
					,DATEDIFF(day, LastCrawle,SYSUTCDATETIME()) DaySinceLastCrawle, DATEDIFF(hour, LastCrawle,SYSUTCDATETIME()) HourSinceLastCrawle
					,m.HiddenServiceMain
				FROM Pages p WITH (NOLOCK)
					LEFT JOIN HiddenServiceMirrors m WITH (NOLOCK) ON m.HiddenService=p.HiddenService
				WHERE p.Url LIKE '%'+@Keywords+'%'
				ORDER BY p.Rank DESC
				OFFSET @Page*@ResultPerPage ROWS FETCH NEXT @ResultPerPage ROWS ONLY

END
GO
GRANT EXECUTE ON SearchUrl TO sqlReader
GO

CREATE OR ALTER   PROCEDURE [dbo].[SearchSite](@Url NVARCHAR(37), @Page SMALLINT=0) AS  -- have to set a length for @Keywords else bugous results
BEGIN
    SET NOCOUNT ON
	DECLARE @ResultPerPage INT = 10 -- FREETEXTTABLE from 0 to 1000 , set as 0 to 2 versus the 0 to 1 of the Page.Rank
	
	DECLARE @MaxResult INT
	SELECT @MaxResult=COUNT(1) FROM Pages WITH (NOLOCK) WHERE HiddenService=@Url
	SELECT @MaxResult -- returned no called

	IF (@MaxResult>0)

		SELECT Url, COALESCE(Title, p.HiddenService) Title, Heading, CrawleError
					,DATEDIFF(day, LastCrawle,SYSUTCDATETIME()) DaySinceLastCrawle, DATEDIFF(hour, LastCrawle,SYSUTCDATETIME()) HourSinceLastCrawle
					,m.HiddenServiceMain 
					--,[Rank], [RankDate]
				FROM Pages p WITH (NOLOCK)
					LEFT JOIN HiddenServiceMirrors m WITH (NOLOCK) ON m.HiddenService=p.HiddenService
				WHERE p.HiddenService=@Url
				ORDER BY p.Rank DESC
				OFFSET @Page*@ResultPerPage ROWS FETCH NEXT @ResultPerPage ROWS ONLY
END
GO
GRANT EXECUTE ON [SearchSite] TO sqlReader
GO

CREATE OR ALTER   PROCEDURE [dbo].[SearchCached](@Url NVARCHAR(450)) AS  -- have to set a length for @Keywords else bugous results
BEGIN
    SET NOCOUNT ON

	DECLARE @MaxResult INT
	SELECT @MaxResult=COUNT(1) FROM Pages WITH (NOLOCK) WHERE url=@Url
	SELECT @MaxResult -- returned no called

	IF (@MaxResult=1)

		SELECT Url, COALESCE(Title, p.HiddenService) Title, InnerText, CrawleError
					,DATEDIFF(day, LastCrawle,SYSUTCDATETIME()) DaySinceLastCrawle, DATEDIFF(hour, LastCrawle,SYSUTCDATETIME()) HourSinceLastCrawle
					,m.HiddenServiceMain
				FROM Pages p WITH (NOLOCK)
					LEFT JOIN HiddenServiceMirrors m WITH (NOLOCK) ON m.HiddenService=p.HiddenService
				WHERE p.Url=@Url
END
GRANT EXECUTE ON [SearchCached] TO sqlReader
GO
