CREATE OR ALTER VIEW #WsToClRepoSchema.WrkVwClMigrTblHdr 
(
	ClientCd, 
	TblName, 
	TblId, 
	InsertStrategy, 
	IsTestsEnabled, 
	IsTruncateBefore, 
	TblDatabase, 
	IsDeployEnabled, 
	TblDescription, 
	IsDataVault, 
	IsMultiSource, 
	LocationName, 
	Materialization, 
	TblType, 
	RowCreatedTs, 
	RowCreatedBy
)
AS 
-- NON SOURCE MAPPING OBJECTS
SELECT 
	'#ClientCode',
	TblName,
	LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),1,4)) + '-' +
	LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),5,4)) + '-' +
	LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),9,4)) + '-' +
	LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),13,4)) + '-' +
	LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),17,4)) + '-' +
	LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),21,4)) + '-' +
	LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),25,4)) + '-' +
	LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),29,4)) AS TblId,
	InsertStrategy,
	'false' AS TestsEnabled,
	LOWER(TruncateBefore),
	TargetDb AS TblDatabase,
	'false' AS DeployEnabled,
	COALESCE(TblDescription,'') AS TblDescription,
	LOWER(IsDataVault),
	LOWER(IsMultiSource),
	LocationName,
	LOWER(Materialization),
	ObjectType AS TblType,
	GETDATE() AS RowCreatedTs,
	'DEA-PS-WS-Repo-To-CL-Repo.psm1' AS RowCreatedBy
FROM #WsToClRepoSchema.WrkVwClMigrTblHdr05
WHERE Rnk = 1
UNION ALL
-- SOURCE MAPPING OBJECTS
SELECT 
	'#ClientCode',
	TblName,
	LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),1,4)) + '-' +
	LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),5,4)) + '-' +
	LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),9,4)) + '-' +
	LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),13,4)) + '-' +
	LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),17,4)) + '-' +
	LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),21,4)) + '-' +
	LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),25,4)) + '-' +
	LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),29,4)) AS TblId,
	InsertStrategy,
	'false' AS TestsEnabled,
	LOWER(TruncateBefore),
	TargetDb AS TblDatabase,
	'false' AS DeployEnabled,
	COALESCE(TblDescription,'') AS TblDescription,
	LOWER(IsDataVault),
	LOWER(IsMultiSource),
	LocationName,
	LOWER(Materialization),
	ObjectType AS TblType,
	GETDATE() AS RowCreatedTs,
	'DEA-PS-WS-Repo-To-CL-Repo.psm1' AS RowCreatedBy
FROM #WsToClRepoSchema.WrkVwClMigrTblHdr10
WHERE Rnk = 1;