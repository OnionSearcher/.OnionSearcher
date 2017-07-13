CREATE OR ALTER PROCEDURE CheckCanCrawle(@Url NVARCHAR(450), @ret SMALLINT OUTPUT) AS 
BEGIN
    SET NOCOUNT ON
	DECLARE @MaxPagesPerHiddenService INT=10000 -- go to 9000 after a purge per 1000
	DECLARE @MaxRefreshPage INT=72
	DECLARE @MaxRefreshRoot INT=12
	DECLARE @HiddenService NVARCHAR(37)
	SELECT @HiddenService=SUBSTRING(@Url,0,CHARINDEX ('/', @Url,30)+1)
	
	IF NOT EXISTS (SELECT 1 FROM HiddenServices WITH (NOLOCK) WHERE HiddenService=@HiddenService AND @MaxPagesPerHiddenService<IndexedPages) -- strict > because purge is >=
	BEGIN

		DECLARE @lastCrawle DATETIMEOFFSET
		SELECT @lastCrawle=LastCrawle FROM Pages WITH (NOLOCK) WHERE Url=@Url  
		IF @@ROWCOUNT=1
		BEGIN
			IF (@Url<>@HiddenService)
				SET @ret=CASE WHEN DATEADD(hh, @MaxRefreshPage, @lastCrawle)<SYSUTCDATETIME() THEN 1 ELSE 0 END
			ELSE
				SET @ret=CASE WHEN DATEADD(hh, @MaxRefreshRoot, @lastCrawle)<SYSUTCDATETIME() THEN 1 ELSE 0 END
		END
		ELSE
		BEGIN
			SET @ret=1
			SELECT @ret=0
				WHERE EXISTS (SELECT 1 FROM BannedUrl WHERE @Url LIKE UrlLike)
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

CREATE MESSAGE TYPE CrawleRequestType VALIDATION=NONE
GO
CREATE CONTRACT CrawleRequestP1UserPageContract (CrawleRequestType SENT BY INITIATOR)
CREATE CONTRACT CrawleRequestP2N1PageContract (CrawleRequestType SENT BY INITIATOR)
CREATE CONTRACT CrawleRequestP3ExternalLinkContract (CrawleRequestType SENT BY INITIATOR)
CREATE CONTRACT CrawleRequestP4N2PageContract (CrawleRequestType SENT BY INITIATOR)
CREATE CONTRACT CrawleRequestP5N3PageContract (CrawleRequestType SENT BY INITIATOR)
CREATE CONTRACT CrawleRequestP6RetryContract (CrawleRequestType SENT BY INITIATOR)
GO
CREATE QUEUE CrawleRequestsInitiatorQueue WITH STATUS=ON, RETENTION=OFF, POISON_MESSAGE_HANDLING (STATUS=ON) 
CREATE SERVICE CrawleRequestInitiatorService ON QUEUE CrawleRequestsInitiatorQueue
GO
CREATE QUEUE CrawleRequestsTargetQueue WITH STATUS=ON, RETENTION=OFF, POISON_MESSAGE_HANDLING (STATUS=ON) 
CREATE SERVICE CrawleRequestTargetService ON QUEUE CrawleRequestsTargetQueue (CrawleRequestP1UserPageContract,CrawleRequestP2N1PageContract,CrawleRequestP3ExternalLinkContract,CrawleRequestP4N2PageContract,CrawleRequestP5N3PageContract,CrawleRequestP6RetryContract)
GO
CREATE BROKER PRIORITY CrawleRequestP1UserPagePriority FOR CONVERSATION 
SET (CONTRACT_NAME=CrawleRequestP1UserPageContract, LOCAL_SERVICE_NAME=ANY, REMOTE_SERVICE_NAME='ANY', PRIORITY_LEVEL=9)
CREATE BROKER PRIORITY CrawleRequestP2N1PagePriority FOR CONVERSATION  
SET (CONTRACT_NAME=CrawleRequestP2N1PageContract, LOCAL_SERVICE_NAME=ANY, REMOTE_SERVICE_NAME='ANY', PRIORITY_LEVEL=8)
CREATE BROKER PRIORITY CrawleRequestP3ExternalLinkPriority FOR CONVERSATION  
SET (CONTRACT_NAME=CrawleRequestP3ExternalLinkContract, LOCAL_SERVICE_NAME=ANY, REMOTE_SERVICE_NAME='ANY', PRIORITY_LEVEL=6)
CREATE BROKER PRIORITY CrawleRequestP4N2PagePriority FOR CONVERSATION  
SET (CONTRACT_NAME=CrawleRequestP4N2PageContract, LOCAL_SERVICE_NAME=ANY, REMOTE_SERVICE_NAME='ANY', PRIORITY_LEVEL=4)
CREATE BROKER PRIORITY CrawleRequestP5N3PagePriority FOR CONVERSATION  
SET (CONTRACT_NAME=CrawleRequestP5N3PageContract, LOCAL_SERVICE_NAME=ANY, REMOTE_SERVICE_NAME='ANY', PRIORITY_LEVEL=2)
CREATE BROKER PRIORITY CrawleRequestP6RetryPriority FOR CONVERSATION  
SET (CONTRACT_NAME=CrawleRequestP6RetryContract, LOCAL_SERVICE_NAME=ANY, REMOTE_SERVICE_NAME='ANY', PRIORITY_LEVEL=1)
GO

CREATE OR ALTER PROCEDURE CrawleRequestEnqueue(@Url NVARCHAR(450), @prio SMALLINT) AS 
BEGIN
    SET NOCOUNT ON
	DECLARE @InitDlgHandle UNIQUEIDENTIFIER

	DECLARE @canCrawle SMALLINT
	EXEC CheckCanCrawle @Url, @canCrawle OUTPUT
	IF @canCrawle=1
	BEGIN
		IF @prio=1
			BEGIN DIALOG @InitDlgHandle FROM SERVICE CrawleRequestInitiatorService TO SERVICE 'CrawleRequestTargetService' ON CONTRACT CrawleRequestP1UserPageContract WITH LIFETIME=2678400, ENCRYPTION=OFF; -- 60*60*24*31
		ELSE IF @prio=2
			BEGIN DIALOG @InitDlgHandle FROM SERVICE CrawleRequestInitiatorService TO SERVICE 'CrawleRequestTargetService' ON CONTRACT CrawleRequestP2N1PageContract WITH LIFETIME=2678400, ENCRYPTION=OFF; -- 60*60*24*31
		ELSE IF @prio=3
			BEGIN DIALOG @InitDlgHandle FROM SERVICE CrawleRequestInitiatorService TO SERVICE 'CrawleRequestTargetService' ON CONTRACT CrawleRequestP3ExternalLinkContract WITH LIFETIME=1209600, ENCRYPTION=OFF; -- 60*60*24*14
		ELSE IF @prio=4
			BEGIN DIALOG @InitDlgHandle FROM SERVICE CrawleRequestInitiatorService TO SERVICE 'CrawleRequestTargetService' ON CONTRACT CrawleRequestP4N2PageContract WITH LIFETIME=604800, ENCRYPTION=OFF; -- 60*60*24*7
		ELSE IF @prio=5
			BEGIN DIALOG @InitDlgHandle FROM SERVICE CrawleRequestInitiatorService TO SERVICE 'CrawleRequestTargetService' ON CONTRACT CrawleRequestP5N3PageContract WITH LIFETIME=172800, ENCRYPTION=OFF; -- 60*60*24*2
		ELSE -- @prio=6
			BEGIN DIALOG @InitDlgHandle FROM SERVICE CrawleRequestInitiatorService TO SERVICE 'CrawleRequestTargetService' ON CONTRACT CrawleRequestP6RetryContract WITH LIFETIME=86400, ENCRYPTION=OFF; -- 60*60*24*1

		SEND ON CONVERSATION @InitDlgHandle MESSAGE TYPE CrawleRequestType(@Url)
	END
END
GO
GRANT EXECUTE ON CrawleRequestEnqueue TO sqlWriter
GRANT EXECUTE ON CrawleRequestEnqueue TO sqlReader
GO

CREATE OR ALTER PROCEDURE CrawleRequestDequeue(@Url NVARCHAR(450) OUTPUT) AS 
BEGIN
    SET NOCOUNT ON
	DECLARE @canCrawle SMALLINT=0
	DECLARE @RecvReplyDlgHandle UNIQUEIDENTIFIER
	SET @Url=NULL

	WHILE (@canCrawle=0)
	BEGIN
		RECEIVE TOP(1) @Url=message_body, @RecvReplyDlgHandle=conversation_handle FROM CrawleRequestsTargetQueue --ORDER BY [priority] DESC, NEWID() -- avoid sending a lot of request on the same server in same time

		IF @Url IS NOT NULL
		BEGIN
			END CONVERSATION @RecvReplyDlgHandle
			EXEC CheckCanCrawle @Url, @canCrawle OUTPUT
		END
		ELSE
			SET @canCrawle=1 -- exit loop with no result

	END
END
GO
GRANT EXECUTE ON CrawleRequestDequeue TO sqlWriter
GO




DECLARE @canCrawle NVARCHAR(450)
exec CrawleRequestDequeue @canCrawle OUTPUT
SELECT @canCrawle


declare @conversation uniqueidentifier
while exists (select 1 from sys.transmission_queue )
begin
set @conversation=(select top 1 conversation_handle from sys.transmission_queue )
end conversation @conversation with cleanup
end

declare @conversation uniqueidentifier
while exists (select 1 FROM [websearcher-sql].[dbo].CrawleRequestsInitiatorQueue WITH(NOLOCK) )
begin
set @conversation=(select top 1 conversation_handle FROM [websearcher-sql].[dbo].CrawleRequestsInitiatorQueue WITH(NOLOCK) )
end conversation @conversation with cleanup
end


SELECT conversation_handle FROM [websearcher-sql].[dbo].CrawleRequestsInitiatorQueue WITH(NOLOCK)

