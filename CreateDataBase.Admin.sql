CREATE OR ALTER PROCEDURE DbCleanTask
AS
BEGIN
    SET NOCOUNT ON
	DECLARE @ret SMALLINT
	
	SELECT @ret=1
	WHILE @ret = 1
	BEGIN
		PRINT 'MirrorsDetectTask - ' + CAST(CURRENT_TIMESTAMP AS VARCHAR)
		EXEC MirrorsDetectTask @ret OUT
		WAITFOR DELAY '00:00:01'
	END

	PRINT 'ComputeIndexedPagesTask - ' + CAST(CURRENT_TIMESTAMP AS VARCHAR)
	EXEC ComputeIndexedPagesTask
	WAITFOR DELAY '00:00:01'
	
	SELECT @ret=1
	WHILE @ret = 1
	BEGIN
		PRINT 'UpdateHiddenServicesRankTask - ' + CAST(CURRENT_TIMESTAMP AS VARCHAR)
		EXEC UpdateHiddenServicesRankTask @ret OUT
		WAITFOR DELAY '00:00:01'
	END
	
	SELECT @ret=1
	WHILE @ret = 1
	BEGIN
		PRINT 'UpdatePageRankTask - ' + CAST(CURRENT_TIMESTAMP AS VARCHAR)
		EXEC UpdatePageRankTask @ret OUT
		WAITFOR DELAY '00:00:01'
	END
	
	SELECT @ret=1
	WHILE @ret = 1
	BEGIN
		PRINT 'PagesPurgeTask - ' + CAST(CURRENT_TIMESTAMP AS VARCHAR)
		EXEC PagesPurgeTask @ret OUT
		WAITFOR DELAY '00:00:01'
	END

	PRINT 'Pages REORGANIZE - ' + CAST(CURRENT_TIMESTAMP AS VARCHAR)
	ALTER INDEX ALL ON Pages REORGANIZE
	-- ALTER INDEX ALL ON Pages REBUILD -- offline
	WAITFOR DELAY '00:00:01'

	PRINT 'HiddenServices REORGANIZE - ' + CAST(CURRENT_TIMESTAMP AS VARCHAR)
	ALTER INDEX ALL ON HiddenServices REORGANIZE
	-- ALTER INDEX ALL ON HiddenServices REBUILD -- offline
	WAITFOR DELAY '00:00:01'
	
	PRINT 'BannedPages REORGANIZE - ' + CAST(CURRENT_TIMESTAMP AS VARCHAR)
	ALTER INDEX ALL ON BannedPages REORGANIZE
	-- ALTER INDEX ALL ON BannedPages REBUILD -- offline
	WAITFOR DELAY '00:00:01'
	
	PRINT 'HiddenServiceLinks REORGANIZE - ' + CAST(CURRENT_TIMESTAMP AS VARCHAR)
	ALTER INDEX ALL ON HiddenServiceLinks REORGANIZE
	-- ALTER INDEX ALL ON HiddenServiceLinks REBUILD -- offline
	WAITFOR DELAY '00:00:01'

	PRINT 'HiddenServiceMirrors REORGANIZE - ' + CAST(CURRENT_TIMESTAMP AS VARCHAR)
	ALTER INDEX ALL ON HiddenServiceMirrors REORGANIZE
	-- ALTER INDEX ALL ON HiddenServiceMirrors REBUILD -- offline
	WAITFOR DELAY '00:00:01'
	
	PRINT 'SearchCatalog REORGANIZE - ' + CAST(CURRENT_TIMESTAMP AS VARCHAR)
	ALTER FULLTEXT CATALOG SearchCatalog REORGANIZE
	-- ALTER FULLTEXT CATALOG SearchCatalog REBUILD 
	-- ALTER FULLTEXT INDEX ON Pages START FULL POPULATION
	WAITFOR DELAY '00:00:01'

	PRINT 'UPDATESTATS - ' + CAST(CURRENT_TIMESTAMP AS VARCHAR)
	EXEC sp_updatestats

	PRINT 'END - ' + CAST(CURRENT_TIMESTAMP AS VARCHAR)
END
GO
-- need to be admin to do EXEC DbCleanTask

--------------------------------------------------------------------------------------------------------------
/*** Tools ***/

-- Trailling slash cleanup
update p SET URL = SUBSTRING(URL, 0, len(URL)) from pages p where HiddenService<>url and url like '%/' and not exists (SELECT 1 FROM Pages s WHERE s.URL = SUBSTRING(p.URL,0,len(p.URL)))
DELETE from pages where HiddenService<>url and url like '%/'

-- Trailling ? cleanup
update p SET URL = SUBSTRING(URL, 0, len(URL)) from pages p where HiddenService<>url and url like '?/' and not exists (SELECT 1 FROM Pages s WHERE s.URL = SUBSTRING(p.URL,0,len(p.URL)))
DELETE from pages where HiddenService<>url and url like '?/'

-- Stats
select p.Title, s.* FROM (
SELECT HiddenService, count(1) pages, min(url) minUrl, max(url) maxUrl, min(CrawleError) minCrawleError, max(CrawleError) maxCrawleError, min(FirstCrawle) FirstCrawle, max(LastCrawle) LastCrawle from Pages WITH (NOLOCK) group by HiddenService having count(1)>2
)s INNER JOIN Pages p WITH (NOLOCK) ON s.HiddenService = p.Url ORDER BY 1 desc
-- hd up
SELECT COUNT(1) FROM Pages p WITH (NOLOCK)
WHERE p.HiddenService=p.Url AND p.CrawleError IS NULL

FROM (
SELECT HiddenService, count(1) pages, min(url) minUrl, max(url) maxUrl, min(CrawleError) minCrawleError, max(CrawleError) maxCrawleError, min(FirstCrawle) FirstCrawle, max(LastCrawle) LastCrawle from Pages WITH (NOLOCK) group by HiddenService having count(1)>2
)s INNER JOIN  ON s.HiddenService = p.Url ORDER BY 1 desc


EXEC sp_spaceused N'Pages'
EXEC sp_spaceused N'HiddenServices'


-- LookForUrlStopperCandidate
SELECT Query, COUNT(DISTINCT HiddenService) As HiddenServiceCount, COUNT(1) AS UrlCount, MIN(Url) AS MinUrl, MAX(Url) AS MaxUrl FROM (
	SELECT HiddenService, Url ,SUBSTRING(Url, CHARINDEX('?',Url,30), CHARINDEX('=',Url,CHARINDEX('?',Url,30))-CHARINDEX('?',Url,30)+1) as Query FROM Pages WITH (NOLOCK) WHERE Url LIKE '%?%=%'-- AND Url NOT LIKE '%?'
	) s GROUP BY Query ORDER BY 2 DESC

-- LookForHiddenServiceMirrorsCandidate AS
SELECT t.Title,SUBSTRING(p.InnerText,0,128) InnerText,  p.Url, p.CrawleError
		,(SELECT COUNT(1) FROM HiddenServiceLinks WHERE HiddenServiceTarget = p.url) HasHiddenServiceTarget
		,m1.HiddenServiceMain as AlreadyMirrorOf
		,(SELECT TOP 1 HiddenServiceMain FROM HiddenServiceMirrors m2 WHERE p.Url=m2.HiddenServiceMain) AlreadyMirrorMain
	FROM (SELECT Title, COUNT(1) NbHiddenServices FROM Pages WITH (NOLOCK) WHERE Url=HiddenService GROUP BY Title HAVING COUNT(1)>1) t
		INNER JOIN Pages p WITH (NOLOCK) ON t.Title=p.Title
		LEFT JOIN HiddenServiceMirrors m1 ON p.Url = m1.HiddenService
	WHERE p.Url=p.HiddenService
	ORDER BY t.Title ASC, InnerText ASC, AlreadyMirrorMain DESC, HasHiddenServiceTarget DESC
-- in XLS, an added column with the target and another column with the formula : ="INSERT INTO [HiddenServiceMirrors] ([HiddenService],[HiddenServiceMain]) VALUES ('"&C3&"','"&H3&"')"


-- improve mirror
SELECT p.Url, h.[IndexedPages],h.[Rank], p.[Rank], p.crawleError, p.title, p.heading
,(SELECT COUNT(1) FROM [HiddenServiceMirrors] WHERE [HiddenServiceMain]=p.Url) HasMirror
,(SELECT COUNT(1) FROM [HiddenServiceMirrors] WHERE [HiddenService]=p.Url) WARN_AsAMirror
FROM Pages p WITH (NOLOCK)
INNER JOIN HiddenServices h WITH (NOLOCK) ON h.HiddenService = p.Url
WHERE Url IN (SELECT distinct HiddenServiceMain FROM HiddenServiceMirrors WITH (NOLOCK))
ORDER BY Title, h.[Rank] DESC
