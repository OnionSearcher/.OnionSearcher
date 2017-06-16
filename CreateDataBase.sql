CREATE USER sqlReader FROM LOGIN sqlReader
CREATE USER sqlWriter FROM LOGIN sqlWriter
GRANT SELECT TO sqlReader
GRANT SELECT, INSERT, UPDATE, DELETE TO sqlWriter

------------------------------
IF EXISTS (SELECT 1 FROM sys.fulltext_index_catalog_usages)   
	DROP FULLTEXT INDEX ON Pages
IF OBJECT_ID('dbo.OuterLinks') IS NOT NULL
	DROP TABLE OuterLinks
IF OBJECT_ID('dbo.InnerLinks') IS NOT NULL
	DROP TABLE InnerLinks
IF OBJECT_ID('dbo.Pages') IS NOT NULL
	DROP TABLE Pages
IF EXISTS (SELECT 1 FROM sys.fulltext_catalogs WHERE Name = 'PageSearchCatalog')   
	DROP FULLTEXT CATALOG SearchCatalog

CREATE FULLTEXT CATALOG SearchCatalog WITH ACCENT_SENSITIVITY=OFF;

CREATE TABLE Pages
(
	HiddenService NVARCHAR(450) NOT NULL,
	Url NVARCHAR(450) NOT NULL,
	Title NVARCHAR(450),
	InnerText NVARCHAR(MAX),
	InnerLinks NVARCHAR(MAX),
	OuterLinks NVARCHAR(MAX),
	FirstCrawle DATETIMEOFFSET NOT NULL,
	LastCrawle DATETIMEOFFSET NOT NULL,
	CrawleError SMALLINT,
	Rank FLOAT NOT NULL DEFAULT 0.2,
	RankDate DATETIMEOFFSET
)
ALTER TABLE Pages ADD CONSTRAINT PK_Pages PRIMARY KEY CLUSTERED (Url)

CREATE FULLTEXT INDEX ON Pages  
(   
	Url Language 1033, 
	Title Language 1033,  
	InnerText Language 1033
)   
KEY INDEX PK_Pages ON SearchCatalog
WITH STOPLIST = SYSTEM, CHANGE_TRACKING AUTO

CREATE TABLE BannedPages
(
	Url NVARCHAR(450) NOT NULL,
	Reason NVARCHAR(64) NOT NULL,
	PagesPurge DATETIMEOFFSET
)
ALTER TABLE BannedPages ADD CONSTRAINT PK_BannedPages PRIMARY KEY CLUSTERED (Url)

CREATE TABLE HiddenServices
(
	HiddenService NVARCHAR(450) NOT NULL,
	IndexedPages INT,
	ReferredByHiddenServices INT,
	ReferredByPages INT,
	Rank FLOAT NOT NULL DEFAULT 0.2,
	RankDate DATETIMEOFFSET
)
ALTER TABLE HiddenServices ADD CONSTRAINT PK_HiddenServices PRIMARY KEY CLUSTERED (HiddenService)

CREATE TABLE HiddenServiceMirrors
(
	HiddenService NVARCHAR(450) NOT NULL,
	HiddenServiceMain NVARCHAR(450) NOT NULL
)
ALTER TABLE HiddenServiceMirrors ADD CONSTRAINT PK_HiddenServiceMirrors PRIMARY KEY CLUSTERED (HiddenService)

CREATE VIEW LookForUrlStopperCandidate AS
	SELECT Query, COUNT(DISTINCT HiddenService) As HiddenServiceCount, COUNT(1) AS UrlCount, MIN(Url) AS MinUrl, MAX(Url) AS MaxUrl FROM (
	SELECT HiddenService, Url ,SUBSTRING(Url, CHARINDEX('?',Url,30), CHARINDEX('=',Url,CHARINDEX('?',Url,30))-CHARINDEX('?',Url,30)+1) as Query FROM Pages WITH (NOLOCK) WHERE Url LIKE '%?%=%'-- AND Url NOT LIKE '%?'
	) s GROUP BY Query --ORDER BY 2 DESC
GO

CREATE OR ALTER PROCEDURE UpdateHiddenServicesRankTask(@ret SMALLINT OUTPUT) AS 
BEGIN
    SET NOCOUNT ON
	SET @ret=0

	DECLARE @MinHiddenServicesRankRefreshHours INT = 48

	DECLARE @hiddenService NVARCHAR(450)
	SELECT TOP 1 @hiddenService=HiddenService FROM HiddenServices WITH (NOLOCK)
		WHERE RankDate IS NULL OR DATEADD(hh, @MinHiddenServicesRankRefreshHours, RankDate) < SYSDATETIMEOFFSET()

	IF @@ROWCOUNT=1
	BEGIN

		IF OBJECT_ID('tempdb..#RankingHiddenServices') IS NOT NULL
			DROP TABLE #RankingHiddenServices

		SELECT HiddenService, IndexedPages, ReferredByHiddenServices, ReferredByPages INTO #RankingHiddenServices FROM HiddenServices WITH (NOLOCK) WHERE HiddenService = @hiddenService

		UPDATE r -- long time processing, limited update...
			SET IndexedPages = (SELECT COUNT(1) FROM Pages WITH (NOLOCK) WHERE Pages.Url LIKE r.HiddenService +'%') -- faster than a query on the Pages.HiddenService
				,ReferredByPages = COALESCE(s.ReferredByPages,0)
				,ReferredByHiddenServices = COALESCE(s.ReferredByHiddenServices,0)
			FROM #RankingHiddenServices r
				,(
					SELECT COUNT(DISTINCT HiddenService) as ReferredByHiddenServices, COUNT(1) as ReferredByPages
						FROM Pages WITH (NOLOCK)
						WHERE OuterLinks LIKE '%'+@hiddenService+'%'
				) s
		
		DECLARE @HDRefHD_Weighting FLOAT = 2.0
		DECLARE @HDRefPages_Weighting FLOAT = 0.5
	
		DECLARE @HDRefHD_Max FLOAT = 1.0
		DECLARE @HDRefHD_Range INT = 10
		DECLARE @HDRefHD_Min FLOAT = 0.0

		DECLARE @HDRefPages_Max FLOAT = 1.0
		DECLARE @HDRefPages_Range INT = 100
		DECLARE @HDRefPages_Min FLOAT = 0.0

		UPDATE t
			SET RankDate = SYSDATETIMEOFFSET()
				,IndexedPages = r.IndexedPages
				,Rank = (@HDRefHD_Weighting*CASE
					WHEN r.ReferredByHiddenServices>=@HDRefHD_Range THEN @HDRefHD_Max
					ELSE @HDRefHD_Min+(@HDRefHD_Max-@HDRefHD_Min)*(r.ReferredByHiddenServices)/@HDRefHD_Range
				END+@HDRefPages_Weighting*CASE
					WHEN r.ReferredByPages>=@HDRefPages_Range THEN @HDRefPages_Max
					ELSE @HDRefPages_Min+(@HDRefPages_Max-@HDRefPages_Min)*(r.ReferredByPages)/@HDRefPages_Range
				END)/(@HDRefHD_Weighting+@HDRefPages_Weighting)
			FROM HiddenServices t
			INNER JOIN #RankingHiddenServices r ON t.HiddenService=r.HiddenService

		DROP TABLE #RankingHiddenServices
		SELECT @ret=1
			WHERE EXISTS (SELECT 1 FROM HiddenServices WITH (NOLOCK) WHERE RankDate IS NULL OR DATEADD(hh, @MinHiddenServicesRankRefreshHours, RankDate) < SYSDATETIMEOFFSET())
	END
END
GO
GRANT EXECUTE ON UpdateHiddenServicesRankTask TO sqlWriter

CREATE OR ALTER PROCEDURE UpdatePageRankTask(@ret SMALLINT OUTPUT) AS 
BEGIN
    SET NOCOUNT ON
	IF OBJECT_ID('tempdb..#RankingPages') IS NOT NULL
		DROP TABLE #RankingPages
		
	-- Stats related to the page
	
	DECLARE @MinPagesRankRefreshHours INT = 72

	DECLARE @UrlRank_ServiceRoot FLOAT = 1.0
	DECLARE @UrlRank_SubPageMax FLOAT = 0.6
	DECLARE @UrlRank_NbToGetFromMaxToMin INT = 40
	DECLARE @UrlRank_SubPageMin FLOAT = 0.2

	DECLARE @ErrorRank_OK FLOAT = 1.0
	DECLARE @ErrorRank_KOMax FLOAT = 0.6
	DECLARE @ErrorRank_NbToGetFromMaxToMin INT = 5
	DECLARE @ErrorRank_KOMin FLOAT = 0.1

	SELECT TOP 100 Url
			,CASE
				WHEN Url=HiddenService THEN @UrlRank_ServiceRoot
				WHEN LEN(url)-LEN(HiddenService)>=@UrlRank_NbToGetFromMaxToMin THEN @UrlRank_SubPageMin
				ELSE @UrlRank_SubPageMin+(@UrlRank_SubPageMax-@UrlRank_SubPageMin)*(@UrlRank_NbToGetFromMaxToMin-(LEN(Url)-LEN(HiddenService)))/@UrlRank_NbToGetFromMaxToMin
			END AS UrlRank
			,CASE 
				WHEN CrawleError IS NULL THEN @ErrorRank_OK
				WHEN CrawleError>=@ErrorRank_NbToGetFromMaxToMin THEN @ErrorRank_KOMin
				ELSE @ErrorRank_KOMin+(@ErrorRank_KOMax-@ErrorRank_KOMin)*(@ErrorRank_NbToGetFromMaxToMin-CrawleError)/@ErrorRank_NbToGetFromMaxToMin
			END AS ErrorRank
		INTO #RankingPages
		FROM Pages WITH (NOLOCK)
		WHERE RankDate IS NULL OR DATEADD(hh, @MinPagesRankRefreshHours, RankDate) < SYSDATETIMEOFFSET()
		ORDER BY RankDate ASC -- null first

	DECLARE @UrlRank_Weighting FLOAT = 2.0
	DECLARE @ErrorRank_Weighting FLOAT = 1.0
	DECLARE @HDRank_Weighting FLOAT = 2.0

	UPDATE p
		SET RankDate = SYSDATETIMEOFFSET()
			,Rank = (@UrlRank_Weighting*UrlRank+@ErrorRank_Weighting*ErrorRank+@HDRank_Weighting*hd.Rank)/(@UrlRank_Weighting+@ErrorRank_Weighting+@HDRank_Weighting)
		FROM Pages p
			INNER JOIN #RankingPages urp ON p.Url=urp.Url
			INNER JOIN HiddenServices hd ON p.HiddenService=hd.HiddenService

	DROP TABLE #RankingPages
	
	SET @ret=0
	SELECT @ret=1
		WHERE EXISTS (SELECT 1 FROM Pages WITH (NOLOCK) WHERE RankDate IS NULL OR DATEADD(hh, @MinPagesRankRefreshHours, RankDate) < SYSDATETIMEOFFSET())
END
GO
GRANT EXECUTE ON UpdatePageRankTask TO sqlWriter

CREATE OR ALTER PROCEDURE PagesPurgeTask(@ret SMALLINT OUTPUT) AS 
BEGIN
    SET NOCOUNT ON
	SET @ret=0
	-- @MaxPagesPerHiddenService management
	DECLARE @MaxPagesPerHiddenService INT = 10000 -- go to 9000 after a purge per 1000
	DECLARE @hiddenService NVARCHAR(450)
	SELECT TOP 1 @hiddenService=HiddenService FROM Pages WITH (NOLOCK) WHERE RankDate IS NOT NULL GROUP BY HiddenService HAVING COUNT(1)>@MaxPagesPerHiddenService
	IF @@ROWCOUNT=1
	BEGIN
		DELETE FROM Pages WHERE Url IN (SELECT TOP 1000 Url FROM Pages WHERE Url LIKE @hiddenService+'%' AND RankDate IS NOT NULL ORDER BY Rank ASC)  -- will only purge ranked pages
		IF @@ROWCOUNT>0
		BEGIN
			PRINT 'MaxPagesPerHiddenService purged '+CAST(@@ROWCOUNT AS NVARCHAR)+' of ' + @hiddenService
			SET @ret=1
		END
	END
	-- BannedPages management
	IF @ret=0
	BEGIN
		SELECT TOP 1 @hiddenService=Url FROM BannedPages WITH (NOLOCK) WHERE PagesPurge IS NULL
		IF @@ROWCOUNT=1
		BEGIN
			DELETE TOP (1000) FROM Pages WHERE Url like @hiddenService + '%'
			IF @@ROWCOUNT>0
			BEGIN
				PRINT 'BannedPages purged '+CAST(@@ROWCOUNT AS NVARCHAR)+' of ' + @hiddenService
				SET @ret=1
			END
			ELSE 
				UPDATE BannedPages SET PagesPurge=SYSDATETIMEOFFSET() WHERE Url=@hiddenService
		END
	END
	-- Mirror management
	IF @ret=0
	BEGIN
		DELETE FROM Pages WHERE Url IN (SELECT TOP 1000 Url
			FROM HiddenServiceMirrors m WITH (NOLOCK)
			INNER JOIN Pages p1 WITH (NOLOCK) ON p1.URL LIKE m.HiddenService+'%' AND p1.URL<>p1.HiddenService
			WHERE EXISTS (SELECT 1 FROM Pages p2 WHERE p2.Url = REPLACE(p1.Url, m.HiddenService, m.HiddenServiceMain)))
		IF @@ROWCOUNT>0
		BEGIN
			PRINT 'HiddenServiceMirrors purged '+CAST(@@ROWCOUNT AS NVARCHAR)
			SET @ret=1
		END
	END
END
GO
GRANT EXECUTE ON PagesPurgeTask TO sqlWriter

CREATE OR ALTER PROCEDURE DbCleanTask
AS
BEGIN
    SET NOCOUNT ON
	DECLARE @ret SMALLINT
	
	PRINT 'ComputeIndexedPagesTask - ' + CAST(CURRENT_TIMESTAMP AS VARCHAR)
	EXEC ComputeIndexedPagesTask
	
	PRINT 'UpdateHiddenServicesRankTask - ' + CAST(CURRENT_TIMESTAMP AS VARCHAR)
	SELECT @ret=1
	WHILE @ret = 1
	   exec UpdateHiddenServicesRankTask @ret OUT

	PRINT 'UpdatePageRankTask - ' + CAST(CURRENT_TIMESTAMP AS VARCHAR)
	SELECT @ret=1
	WHILE @ret = 1
	   exec UpdatePageRankTask @ret OUT

	PRINT 'PagesPurgeTask - ' + CAST(CURRENT_TIMESTAMP AS VARCHAR)
	SELECT @ret=1
	WHILE @ret = 1
	   exec PagesPurgeTask @ret OUT
	   
	PRINT 'INDEX REORGANIZE - ' + CAST(CURRENT_TIMESTAMP AS VARCHAR)
	ALTER INDEX ALL ON Pages REORGANIZE
	-- ALTER INDEX ALL ON Pages REBUILD -- offline

	PRINT 'SearchCatalog REORGANIZE - ' + CAST(CURRENT_TIMESTAMP AS VARCHAR)
	ALTER FULLTEXT CATALOG SearchCatalog REORGANIZE
	-- ALTER FULLTEXT CATALOG SearchCatalog REBUILD 
	-- ALTER FULLTEXT INDEX ON Pages START FULL POPULATION
	
	PRINT 'END - ' + CAST(CURRENT_TIMESTAMP AS VARCHAR)
END
GO
-- need tp be admin to execute...

CREATE OR ALTER PROCEDURE ComputeIndexedPagesTask
AS
BEGIN
    SET NOCOUNT ON
	
	-- update basic HiddenServices stats (mandatory for PagesPurge)
	MERGE HiddenServices AS target USING (SELECT HiddenService, COUNT(1) as IndexedPages FROM Pages WITH (NOLOCK) GROUP BY HiddenService) AS source ON (target.HiddenService = source.HiddenService)
		WHEN MATCHED THEN UPDATE SET IndexedPages=source.IndexedPages
		WHEN NOT MATCHED THEN INSERT (HiddenService,IndexedPages) VALUES (source.HiddenService,source.IndexedPages); -- will have the default rank

END
GO
GRANT EXECUTE ON ComputeIndexedPagesTask TO sqlWriter

CREATE OR ALTER PROCEDURE CanCrawle(@Url NVARCHAR(450), @HiddenService NVARCHAR(450), @ret SMALLINT OUTPUT) AS 
BEGIN
    SET NOCOUNT ON
	DECLARE @MaxPagesPerHiddenService INT = 10000 -- go to 9000 after a purge per 1000
	DECLARE @MaxRefreshPage INT = 72
	DECLARE @MaxRefreshRoot INT = 8
	
	IF NOT EXISTS (SELECT 1 FROM HiddenServices WITH (NOLOCK) WHERE HiddenService=@HiddenService AND @MaxPagesPerHiddenService<IndexedPages) -- strict > because purge is >=
	BEGIN

		DECLARE @lastCrawle DATETIMEOFFSET
		SELECT @lastCrawle=LastCrawle FROM Pages WITH (NOLOCK) WHERE Url=@Url  
		IF @@ROWCOUNT=1
		BEGIN
			IF (@Url<>@HiddenService)
				SET @ret=CASE WHEN DATEADD(hh, @MaxRefreshPage, @lastCrawle)<SYSDATETIMEOFFSET() THEN 1 ELSE 0 END
			ELSE
				SET @ret=CASE WHEN DATEADD(hh, @MaxRefreshRoot, @lastCrawle)<SYSDATETIMEOFFSET() THEN 1 ELSE 0 END
		END
		ELSE
		BEGIN
			SET @ret=1
			SELECT @ret=0 WHERE EXISTS(SELECT 1 FROM BannedPages WHERE @Url like Url + '%')
		END

		-- if still seems OK, is it a mirror know (remove only sub pages) ?
		IF @ret=1 AND @Url<>@HiddenService
		BEGIN
			DECLARE @hiddenServiceMain NVARCHAR(450)
			SELECT @hiddenServiceMain=HiddenServiceMain FROM HiddenServiceMirrors WHERE HiddenService=@HiddenService
			IF @@ROWCOUNT=1
			BEGIN
				DECLARE @urlMain NVARCHAR(450)
				SET @urlMain=REPLACE(@Url,@HiddenService,@hiddenServiceMain)
				EXEC CanCrawle @urlMain, @hiddenServiceMain, @ret OUT -- override current search param
			END
		END
	END
	ELSE
		SET @ret=0
END
GO
GRANT EXECUTE ON CanCrawle TO sqlWriter
GRANT EXECUTE ON CanCrawle TO sqlReader

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

CREATE OR ALTER PROCEDURE Search(@Keywords NVARCHAR(64), @Page INT=0) AS
BEGIN
    SET NOCOUNT ON
	DECLARE @FreeTextTable_Weighting FLOAT = 0.002 -- FREETEXTTABLE from 0 to 1000 , set as 0 to 2 versus the 0 to 1 of the Page.Rank
	DECLARE @ResultPerPage INT = 10 -- FREETEXTTABLE from 0 to 1000 , set as 0 to 2 versus the 0 to 1 of the Page.Rank

	SELECT COUNT(1) FROM FREETEXTTABLE(Pages, *, @Keywords) r -- have to set a length for @Keywords else bugous results

	SELECT Url, COALESCE(Title, HiddenService) Title, dbo.TrimTextSearch(@Keywords, InnerText) as InnerText, CrawleError, DATEDIFF(day, LastCrawle,SYSDATETIMEOFFSET()) DaySinceLastCrawle, DATEDIFF(hour, LastCrawle,SYSDATETIMEOFFSET()) HourSinceLastCrawle
			FROM FREETEXTTABLE(Pages, *, @Keywords, LANGUAGE 1033) r
			INNER JOIN Pages p WITH (NOLOCK) ON p.Url=r.[KEY]
			ORDER BY r.RANK*@FREETEXTTABLE_Weighting + p.Rank
			OFFSET @Page*@ResultPerPage ROWS FETCH NEXT @ResultPerPage ROWS ONLY
END
GO
GRANT EXECUTE ON Search TO sqlReader

--------------------------------------------------------------------------------------------------------------
/*** Tools ***/

-- Trailling slash cleanup
update p SET URL = SUBSTRING(URL, 0, len(URL)) from pages p where HiddenService<>url and url like '%/' and not exists (SELECT 1 FROM Pages s WHERE s.URL = SUBSTRING(p.URL,0,len(p.URL)))
DELETE from pages where HiddenService<>url and url like '%/'

-- Trailling ? cleanup
update p SET URL = SUBSTRING(URL, 0, len(URL)) from pages p where HiddenService<>url and url like '?/' and not exists (SELECT 1 FROM Pages s WHERE s.URL = SUBSTRING(p.URL,0,len(p.URL)))
DELETE from pages where HiddenService<>url and url like '?/'

-- UrlStopper cleanup
DELETE FROM Pages WHERE Url like '%?redirect_to=%' OR Url like '%?sort=%' OR Url like '%?currency=%' OR Url like '%?replytocom=%'

-- Stats
select p.Title, s.* FROM (
SELECT HiddenService, count(1) pages, min(url) minUrl, max(url) maxUrl, min(CrawleError) minCrawleError, max(CrawleError) maxCrawleError, min(FirstCrawle) FirstCrawle, max(LastCrawle) LastCrawle from Pages WITH (NOLOCK) group by HiddenService
)s INNER JOIN Pages p WITH (NOLOCK) ON s.HiddenService = p.Url ORDER BY 3 desc

EXEC sp_spaceused N'Pages'
