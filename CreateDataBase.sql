CREATE USER sqlReader FROM LOGIN sqlReader
CREATE USER sqlWriter FROM LOGIN sqlWriter
GRANT SELECT TO sqlReader
GRANT SELECT, INSERT, UPDATE, DELETE TO sqlWriter

CREATE FULLTEXT CATALOG SearchCatalog WITH ACCENT_SENSITIVITY=OFF;

CREATE FULLTEXT STOPLIST SearchStoplist FROM SYSTEM STOPLIST;
GRANT VIEW DEFINITION ON FULLTEXT STOPLIST :: SearchStoplist TO sqlReader;
ALTER FULLTEXT STOPLIST SearchStoplist ADD 'tor' LANGUAGE 'Neutral';
ALTER FULLTEXT STOPLIST SearchStoplist ADD 'tor' LANGUAGE 1033;
ALTER FULLTEXT STOPLIST SearchStoplist ADD 'onion' LANGUAGE 'Neutral';
ALTER FULLTEXT STOPLIST SearchStoplist ADD 'onion' LANGUAGE 1033;
ALTER FULLTEXT STOPLIST SearchStoplist ADD 'http' LANGUAGE 'Neutral';
ALTER FULLTEXT STOPLIST SearchStoplist ADD 'http' LANGUAGE 1033;
ALTER FULLTEXT STOPLIST SearchStoplist ADD 'web' LANGUAGE 'Neutral';
ALTER FULLTEXT STOPLIST SearchStoplist ADD 'web' LANGUAGE 1033;
ALTER FULLTEXT STOPLIST SearchStoplist ADD 'web' LANGUAGE 'Neutral';
ALTER FULLTEXT STOPLIST SearchStoplist ADD 'web' LANGUAGE 1033;
-- stop helping fucking pedo.... (list not commited for not helping giving keywords...)

CREATE TABLE Pages
(
	HiddenService NVARCHAR(37) NOT NULL,
	Url NVARCHAR(450) NOT NULL,
	Title NVARCHAR(450),
	Heading NVARCHAR(450),
	InnerText NVARCHAR(MAX),
	FirstCrawle DATETIMEOFFSET NOT NULL,
	LastCrawle DATETIMEOFFSET NOT NULL,
	CrawleError SMALLINT,
	Rank FLOAT NOT NULL DEFAULT 0.2,
	RankDate DATETIMEOFFSET
)
ALTER TABLE Pages ADD CONSTRAINT PK_Pages PRIMARY KEY CLUSTERED (Url)

CREATE FULLTEXT INDEX ON Pages  
(   
	Title Language 1033, -- Statistical_Semantics : doesn't work on azure because no fulltext_semantic_language_statistics_database
	Heading Language 1033, -- Statistical_Semantics : doesn't work on azure because no fulltext_semantic_language_statistics_database
	InnerText Language 1033, -- Statistical_Semantics : doesn't work on azure because no fulltext_semantic_language_statistics_database
)   
KEY INDEX PK_Pages ON SearchCatalog
WITH STOPLIST = SearchStoplist, CHANGE_TRACKING AUTO  
GO 

CREATE TABLE BannedPages
(
	Url NVARCHAR(450) NOT NULL,
	Reason NVARCHAR(64) NOT NULL,
	PagesPurge DATETIMEOFFSET
)
ALTER TABLE BannedPages ADD CONSTRAINT PK_BannedPages PRIMARY KEY CLUSTERED (Url)

CREATE TABLE HiddenServices
(
	HiddenService NVARCHAR(37) NOT NULL,
	IndexedPages INT,
	ReferredByHiddenServices INT,
	ReferredByPages INT,
	Rank FLOAT NOT NULL DEFAULT 0.2,
	RankDate DATETIMEOFFSET
)
ALTER TABLE HiddenServices ADD CONSTRAINT PK_HiddenServices PRIMARY KEY CLUSTERED (HiddenService)

CREATE TABLE HiddenServiceMirrors
(
	HiddenService NVARCHAR(37) NOT NULL,
	HiddenServiceMain NVARCHAR(37) NOT NULL
)
ALTER TABLE HiddenServiceMirrors ADD CONSTRAINT PK_HiddenServiceMirrors PRIMARY KEY CLUSTERED (HiddenService) -- NO FK : HD may be new and not exist

CREATE TABLE HiddenServiceLinks
(
	HiddenService NVARCHAR(37) NOT NULL,
	HiddenServiceTarget NVARCHAR(37) NOT NULL
)
ALTER TABLE HiddenServiceLinks ADD CONSTRAINT PK_HiddenServiceLinks PRIMARY KEY CLUSTERED (HiddenService,HiddenServiceTarget)  -- NO FK : HD may be new and not exist
CREATE NONCLUSTERED INDEX IX_HiddenServiceLinks ON HiddenServiceLinks (HiddenServiceTarget)
