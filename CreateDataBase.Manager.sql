-- directly called from manager
CREATE VIEW HiddenServicesToCrawle AS
	SELECT TOP 100 h.HiddenService
		FROM HiddenServices h WITH (NOLOCK)
			LEFT JOIN Pages p ON p.Url=h.HiddenService
		WHERE DATEADD(hh, 12, p.LastCrawle)<SYSUTCDATETIME()	--= Same as CanCrawle.@MaxRefreshRoot
		ORDER BY p.LastCrawle ASC
GO


-- direct call from manager
CREATE VIEW PagesToCrawle AS
	SELECT TOP 1000 Url
		FROM Pages WITH (NOLOCK)
		WHERE LastCrawle<DATEADD(hh, -72, SYSUTCDATETIME())	--= Same as CanCrawle.@MaxRefreshRoot
		ORDER BY NEWID() -- avoid sending a lot of request on the same server like during initial crawle
GO

CREATE VIEW LinkedMirrorsCandidate AS
	SELECT m.HiddenService,m.HiddenServiceTarget,p1.Title,SUBSTRING(p1.InnerText,0,128) InnerText, SUBSTRING(p2.InnerText ,0,128) AS InnerText2
			,p1.CrawleError,p2.CrawleError AS CrawleError2 -- last should be null
			,r2.HiddenServiceMain AS HiddenServiceMainLoopWarning -- should be null
			,h1.IndexedPages, h2.IndexedPages as IndexedPages2 -- should be >=
			,ROW_NUMBER() OVER(PARTITION BY m.HiddenService ORDER BY h2.IndexedPages DESC) as Pref
		FROM HiddenServiceLinks m
			INNER JOIN Pages p1 ON p1.Url=m.HiddenService
			INNER JOIN Pages p2 ON p2.Url=m.HiddenServiceTarget 
			INNER JOIN HiddenServices h1 ON h1.HiddenService=m.HiddenService
			INNER JOIN HiddenServices h2 ON h2.HiddenService=m.HiddenServiceTarget 
			LEFT JOIN HiddenServiceMirrors r2 ON r2.HiddenService=m.HiddenServiceTarget -- should be empty
		WHERE p1.Title=p2.Title
			AND NOT EXISTS (SELECT 1 FROM HiddenServiceLinks r WHERE r.HiddenServiceTarget=m.HiddenService AND r.HiddenService=m.HiddenServiceTarget) -- revers
			AND NOT EXISTS (SELECT 1 FROM HiddenServiceMirrors r WHERE r.HiddenService=m.HiddenService) -- not exists
GO
CREATE OR ALTER PROCEDURE [MirrorsDetectTask](@ret SMALLINT OUTPUT) AS 
BEGIN
    SET NOCOUNT ON
	
	IF OBJECT_ID('tempdb..#MirrorsDetected') IS NOT NULL
		DROP TABLE #MirrorsDetected
	SELECT TOP 10 HiddenService, HiddenServiceTarget
		INTO #MirrorsDetected
		FROM LinkedMirrorsCandidate
		WHERE SUBSTRING(InnerText,0,32)=SUBSTRING(InnerText2 ,0,32) -- to be safe
			AND CrawleError2 IS NULL AND HiddenServiceMainLoopWarning IS NULL AND IndexedPages<=IndexedPages2 -- not filtered in the view to analyse theses result and be juged
			AND Pref=1 -- may have double, only usefull on this first select, not the finla check
	IF @@ROWCOUNT>0
	BEGIN
			UPDATE HiddenServices SET IndexedPages=1, Rank=-1.0, RankDate=SYSUTCDATETIME()
				WHERE HiddenService IN (SELECT HiddenService FROM #MirrorsDetected) 

			UPDATE HiddenServices SET RankDate= NULL -- will recompute next time
				WHERE HiddenService IN (SELECT HiddenServiceTarget FROM #MirrorsDetected) 
				
			INSERT INTO HiddenServiceMirrors
				SELECT HiddenService, HiddenServiceTarget FROM #MirrorsDetected

			-- HDlink purge fully for old data reprocess, page purge will be done with the pagepurgetask
			DELETE l FROM HiddenServiceLinks l
				WHERE l.HiddenService IN (SELECT HiddenService FROM HiddenServiceMirrors) -- optim
				AND NOT EXISTS (SELECT 1 FROM HiddenServiceMirrors r WHERE  r.HiddenService=l.HiddenService AND r.HiddenServiceMain=l.HiddenServiceTarget) -- keep only the link to the main

		SET @ret=0
		SELECT TOP 1 @ret=1
			FROM LinkedMirrorsCandidate
			WHERE SUBSTRING(InnerText,0,32)=SUBSTRING(InnerText2 ,0,32) -- to be safe
				AND CrawleError2 IS NULL AND HiddenServiceMainLoopWarning IS NULL AND IndexedPages<=IndexedPages2

		IF @ret=0 -- cleanup only at the very last loop
			UPDATE m SET m.HiddenServiceMain=c.HiddenServiceMain
				FROM HiddenServiceMirrors m
				INNER JOIN HiddenServiceMirrors c ON m.HiddenServiceMain=c.HiddenService

			-- Mirrors cleanup
			-- DELETE H FROM [HiddenServiceMirrors] h INNER JOIN Pages a ON a.Url=h.HiddenService INNER JOIN Pages m ON m.Url=h.HiddenServiceMain WHERE a.Title != m.Title
			-- DELETE h FROM HiddenServices h WHERE [Rank]<0 AND NOT EXISTS (SELECT 1 FROM [HiddenServiceMirrors] m WHERE m.[HiddenService]= h.[HiddenService]) -- will be recreated

	END
	ELSE
		SET @ret=0

	DROP TABLE #MirrorsDetected
END
GO
GRANT EXECUTE ON MirrorsDetectTask TO sqlManager
GO


CREATE VIEW HiddenServicesRankToUpdate AS
	-- @MinHiddenServicesRankRefreshHours INT = 48
	SELECT HiddenService, RankDate FROM HiddenServices WITH (NOLOCK)
		WHERE RankDate IS NULL OR DATEADD(hh, 48, RankDate) < SYSUTCDATETIME()
		--ORDER BY 2 ASC
GO
CREATE OR ALTER PROCEDURE UpdateHiddenServicesRankTask(@ret SMALLINT OUTPUT) AS 
BEGIN
    SET NOCOUNT ON

	IF OBJECT_ID('tempdb..#RankingHiddenServices') IS NOT NULL
		DROP TABLE #RankingHiddenServices
	SELECT s.HiddenService, IndexedPages = (SELECT COUNT(1) FROM Pages WITH (NOLOCK) WHERE Pages.HiddenService = s.HiddenService)
			,ReferredByHiddenServices = (SELECT COUNT(1) FROM HiddenServiceLinks WITH (NOLOCK) WHERE HiddenServiceTarget = s.HiddenService)
			,p2.CrawleError, m.HiddenService as Mirror
			,CAST(0 AS FLOAT) as Rank
		INTO #RankingHiddenServices
		FROM (SELECT TOP 1000 HiddenService FROM HiddenServicesRankToUpdate WITH (NOLOCK) ORDER BY RankDate ASC) s
			LEFT JOIN Pages p2 WITH (NOLOCK) ON p2.Url=s.HiddenService
			LEFT JOIN HiddenServiceMirrors m ON m.HiddenService=s.HiddenService
	
	IF @@ROWCOUNT>0
	BEGIN

			DECLARE @HDRefHD_Max FLOAT = 1.0
			DECLARE @HDRefHD_Range INT = 10
			DECLARE @HDRefHD_Min FLOAT = 0.0

			DECLARE @ErrorRank_OK FLOAT = 1.0
			DECLARE @ErrorRank_KOMax FLOAT = 0.5
			DECLARE @ErrorRank_NbToGetFromMaxToMin INT = 3
			DECLARE @ErrorRank_KOMin FLOAT = 0.0

			UPDATE #RankingHiddenServices
				SET Rank = CASE WHEN Mirror IS NULL
					THEN(
						CASE
							WHEN ReferredByHiddenServices>=@HDRefHD_Range THEN @HDRefHD_Max
							ELSE @HDRefHD_Min+(@HDRefHD_Max-@HDRefHD_Min)*(ReferredByHiddenServices)/@HDRefHD_Range
						END)
						*(CASE 
							WHEN CrawleError IS NULL THEN @ErrorRank_OK
							WHEN CrawleError>=@ErrorRank_NbToGetFromMaxToMin THEN @ErrorRank_KOMin
							ELSE @ErrorRank_KOMin+(@ErrorRank_KOMax-@ErrorRank_KOMin)*(@ErrorRank_NbToGetFromMaxToMin-CrawleError)/@ErrorRank_NbToGetFromMaxToMin
						END)
					ELSE -1.0 END
			
			 -- limit rowlock time
			UPDATE t
				SET RankDate = SYSUTCDATETIME()
					,IndexedPages = r.IndexedPages
					,Rank = r.Rank
				FROM HiddenServices t
				INNER JOIN #RankingHiddenServices r ON r.HiddenService=t.HiddenService

		SELECT TOP 1 @ret=1
			FROM HiddenServicesRankToUpdate
	END
	ELSE
		SET @ret=0

	DROP TABLE #RankingHiddenServices
END
GO
GRANT EXECUTE ON UpdateHiddenServicesRankTask TO sqlManager
GO


CREATE VIEW PageRankToUpdate AS
	-- DECLARE @MinPagesRankRefreshHours INT = 72
	SELECT Url, HiddenService, RankDate, CrawleError FROM Pages WITH (NOLOCK)
		WHERE (RankDate IS NULL OR DATEADD(hh, 72, RankDate) < SYSUTCDATETIME()) AND (TITLE IS NOT NULL) -- title is null when never scanned, so stay at a 0 rank 
		--ORDER BY 2 ASC
GO
CREATE OR ALTER PROCEDURE UpdatePageRankTask(@ret SMALLINT OUTPUT) AS 
BEGIN
    SET NOCOUNT ON

	IF OBJECT_ID('tempdb..#RankingPages') IS NOT NULL
		DROP TABLE #RankingPages
	SELECT TOP 1000 Url, p.HiddenService, CrawleError, COALESCE(hd.Rank, 0.2) Rank -- hd may not exists, use default
		INTO #RankingPages
		FROM PageRankToUpdate p
			LEFT JOIN HiddenServices hd ON p.HiddenService=hd.HiddenService
		 ORDER BY p.RankDate ASC -- null first

	IF @@ROWCOUNT>0
	BEGIN
	
		-- Stats related to the page
	
		DECLARE @UrlRank_ServiceRoot FLOAT = 1.0
		DECLARE @UrlRank_SubPageMax FLOAT = 0.6
		DECLARE @UrlRank_NbToGetFromMaxToMin INT = 40
		DECLARE @UrlRank_SubPageMin FLOAT = 0.2

		DECLARE @ErrorRank_OK FLOAT = 1.0
		DECLARE @ErrorRank_KOMax FLOAT = 0.6
		DECLARE @ErrorRank_NbToGetFromMaxToMin INT = 5
		DECLARE @ErrorRank_KOMin FLOAT = 0.1

		UPDATE #RankingPages
			SET Rank = Rank*(
				CASE
					WHEN Url=HiddenService THEN @UrlRank_ServiceRoot
					WHEN LEN(url)-LEN(HiddenService)>=@UrlRank_NbToGetFromMaxToMin THEN @UrlRank_SubPageMin
					ELSE @UrlRank_SubPageMin+(@UrlRank_SubPageMax-@UrlRank_SubPageMin)*(@UrlRank_NbToGetFromMaxToMin-(LEN(Url)-LEN(HiddenService)))/@UrlRank_NbToGetFromMaxToMin
				END)
				*(CASE 
					WHEN CrawleError IS NULL THEN @ErrorRank_OK
					WHEN CrawleError>=@ErrorRank_NbToGetFromMaxToMin THEN @ErrorRank_KOMin
					ELSE @ErrorRank_KOMin+(@ErrorRank_KOMax-@ErrorRank_KOMin)*(@ErrorRank_NbToGetFromMaxToMin-CrawleError)/@ErrorRank_NbToGetFromMaxToMin
				END)
			WHERE Rank>0.0 -- useless to compute if already a domain mirror
			
		-- limit rowlock time
		UPDATE p
			SET RankDate = SYSUTCDATETIME()
				,Rank = urp.Rank
			FROM Pages p
				INNER JOIN #RankingPages urp ON p.Url=urp.Url

		SELECT TOP 1 @ret=1 FROM PageRankToUpdate
	END 
	ELSE
		SET @ret=0

	DROP TABLE #RankingPages
END
GO
GRANT EXECUTE ON UpdatePageRankTask TO sqlManager
GO


CREATE OR ALTER PROCEDURE PagesPurgeTask(@ret SMALLINT OUTPUT) AS 
BEGIN
    SET NOCOUNT ON
	SET @ret=0
	-- @MaxPagesPerHiddenService management
	DECLARE @MaxPagesPerHiddenService INT = 10000 -- go to 9000 after a purge per 1000
	DECLARE @url NVARCHAR(450)
	SELECT TOP 1 @url=HiddenService FROM Pages WITH (NOLOCK) WHERE RankDate IS NOT NULL GROUP BY HiddenService HAVING COUNT(1)>@MaxPagesPerHiddenService
	IF @@ROWCOUNT=1
	BEGIN
		SET @ret=1 -- for try next step next run
		DELETE FROM Pages WHERE Url IN (SELECT TOP 1000 Url FROM Pages WHERE HiddenService=@url AND RankDate IS NOT NULL ORDER BY Rank ASC)  -- will only purge ranked pages
	END
	-- Banned management
	IF @ret=0
	BEGIN
		SELECT TOP 1 @url=UrlLike FROM BannedUrl WITH (NOLOCK) WHERE PagesPurge IS NULL
		IF @@ROWCOUNT=1
		BEGIN
			SET @ret=1 -- for try next step next run
			DELETE TOP (1000) FROM Pages WHERE Url LIKE @url
			PRINT @@ROWCOUNT
			IF @@ROWCOUNT=0
				UPDATE BannedUrl SET PagesPurge=SYSUTCDATETIME() WHERE UrlLike=@url
		END
	END
	-- Mirror management
	IF @ret=0
	BEGIN
		PRINT 'Mirror'
		DELETE FROM Pages WHERE Url IN (
			SELECT TOP 1000 Url
				FROM HiddenServiceMirrors m WITH (NOLOCK)
					INNER JOIN Pages p1 WITH (NOLOCK) ON p1.HiddenService=m.HiddenService AND p1.URL<>m.HiddenService
			)
		IF @@ROWCOUNT>0
			SET @ret=1
	END
END
GO
GRANT EXECUTE ON PagesPurgeTask TO sqlManager
GO

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
GRANT EXECUTE ON ComputeIndexedPagesTask TO sqlManager
GO
