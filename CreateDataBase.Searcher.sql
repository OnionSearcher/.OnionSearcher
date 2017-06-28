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
GRANT EXECUTE ON TrimTextSearch TO sqlReader

CREATE OR ALTER PROCEDURE Search2(@Keywords NVARCHAR(64), @Page SMALLINT=0, @Full SMALLINT=0) AS  -- have to set a length for @Keywords else bugous results
BEGIN
    SET NOCOUNT ON
	DECLARE @FreeTextTable_Weighting SMALLINT = 500 -- FREETEXTTABLE from 0 to 1000
	DECLARE @ResultPerPage INT = 10 -- FREETEXTTABLE from 0 to 1000 , set as 0 to 2 versus the 0 to 1 of the Page.Rank
	
	DECLARE @UrlsToGetTenResults INT
	SET @UrlsToGetTenResults = 1000 + 1000 * @Page  -- random value in order to expect just 10 final result for first page !
	
	DECLARE @MaxResult INT
	SELECT COUNT(1) FROM FREETEXTTABLE(Pages, *, @Keywords) r
	SET @MaxResult = @@ROWCOUNT  -- random value in order to expect just 10 final result for first page !

	IF (@Full=0 AND @MaxResult>10) -- the @@ROWCOUNT>10 should be improved because then low result, the group by may be too string
	BEGIN
		SET @MaxResult = CASE WHEN @MaxResult>=@UrlsToGetTenResults THEN 1 WHEN @MaxResult>(100+100*@Page) THEN 2 ELSE 3 END --

		SELECT p2.Url, COALESCE(Title, p2.HiddenService) Title, dbo.TrimTextSearch(@Keywords, InnerText) as InnerText, CrawleError
				,DATEDIFF(day, LastCrawle,SYSDATETIMEOFFSET()) DaySinceLastCrawle, DATEDIFF(hour, LastCrawle,SYSDATETIMEOFFSET()) HourSinceLastCrawle
				,m.HiddenServiceMain
			FROM (
				SELECT  Url, r.RANK+@FREETEXTTABLE_Weighting*p.Rank r, ROW_NUMBER() OVER(PARTITION BY HiddenService ORDER BY (r.RANK+@FREETEXTTABLE_Weighting*p.Rank) DESC) n
					FROM FREETEXTTABLE(Pages, *, @Keywords, LANGUAGE 1033, @UrlsToGetTenResults) r -- 200 : random value in order to expect just 10 final result for first page !
					INNER JOIN Pages p WITH (NOLOCK) ON p.Url=r.[KEY]
				) s
				INNER JOIN Pages p2 ON p2.url=s.url
				LEFT JOIN HiddenServiceMirrors m ON m.HiddenService=s.url
			WHERE s.n<=@MaxResult -- may help a little when low result
			ORDER BY s.r DESC
			OFFSET @Page*@ResultPerPage ROWS FETCH NEXT @ResultPerPage ROWS ONLY

	END
	ELSE

		SELECT Url, COALESCE(Title, p.HiddenService) Title, dbo.TrimTextSearch(@Keywords, InnerText) as InnerText, CrawleError
					,DATEDIFF(day, LastCrawle,SYSDATETIMEOFFSET()) DaySinceLastCrawle, DATEDIFF(hour, LastCrawle,SYSDATETIMEOFFSET()) HourSinceLastCrawle
					,m.HiddenServiceMain
				FROM FREETEXTTABLE(Pages, *, @Keywords, LANGUAGE 1033, @UrlsToGetTenResults) r
					INNER JOIN Pages p WITH (NOLOCK) ON p.Url=r.[KEY]
					LEFT JOIN HiddenServiceMirrors m ON m.HiddenService=r.[KEY]
				ORDER BY r.RANK+@FREETEXTTABLE_Weighting*p.Rank
				OFFSET @Page*@ResultPerPage ROWS FETCH NEXT @ResultPerPage ROWS ONLY

END
GO
GRANT EXECUTE ON Search2 TO sqlReader
