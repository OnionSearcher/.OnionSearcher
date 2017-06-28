
CREATE OR ALTER PROCEDURE CanCrawle(@Url NVARCHAR(450), @HiddenService NVARCHAR(37), @ret SMALLINT OUTPUT) AS 
BEGIN
    SET NOCOUNT ON
	DECLARE @MaxPagesPerHiddenService INT = 10000 -- go to 9000 after a purge per 1000
	DECLARE @MaxRefreshPage INT = 72
	DECLARE @MaxRefreshRoot INT = 12
	
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
			AND EXISTS (SELECT 1 FROM HiddenServiceMirrors WHERE HiddenService=@HiddenService)
				SET @ret=0
	END
	ELSE
		SET @ret=0
END
GO
GRANT EXECUTE ON CanCrawle TO sqlWriter
GRANT EXECUTE ON CanCrawle TO sqlReader
