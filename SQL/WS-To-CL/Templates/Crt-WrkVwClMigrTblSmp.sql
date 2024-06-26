CREATE OR ALTER VIEW #WsToClRepoSchema.WrkVwClMigrTblSmp
(
	ClientCd,
	TblName,
	TblId,	
	SrcTblName,
	SrcTblId,
	CustomSql,
	DepLocName,
	DepNodeName,
	JoinCond,
	SmpName,
	NoLinkLocName,
	NoLinkNodeName,
	RowCreatedTs,
	RowCreatedBy
) AS
-- NON SOURCE MAPPING
SELECT
	'#ClientCode',
	TblName,
	CASE
		WHEN LEN(TRIM(TblName)) > 1
		THEN LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),1,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),5,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),9,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),13,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),17,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),21,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),25,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),29,4))
		ELSE '0'
	END, 
	SrcTblName, 
	CASE
		WHEN LEN(TRIM(SrcTblName)) > 1
		THEN LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',SrcTblName),2),1,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',SrcTblName),2),5,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',SrcTblName),2),9,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',SrcTblName),2),13,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',SrcTblName),2),17,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',SrcTblName),2),21,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',SrcTblName),2),25,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',SrcTblName),2),29,4))
		ELSE '0'
	END, 
	'',
	SrcTblLocation,
	SrcTblName, 
	JoinCond, 
	SmpName,
	'',
	'',
	GETDATE(),
	'DEA-PS-WS-Repo-To-CL-Repo.psm1'
FROM #WsToClRepoSchema.WrkVwClMigrTblSmp05
UNION ALL
-- SOURCE MAPPING
SELECT
	'#ClientCode',
	TblName,
	CASE
		WHEN LEN(TRIM(TblName)) > 1
		THEN LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),1,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),5,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),9,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),13,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),17,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),21,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),25,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',TblName),2),29,4))
		ELSE '0'
	END, 
	SrcTblName, 
	CASE
		WHEN LEN(TRIM(SrcTblName)) > 1
		THEN LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',SrcTblName),2),1,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',SrcTblName),2),5,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',SrcTblName),2),9,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',SrcTblName),2),13,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',SrcTblName),2),17,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',SrcTblName),2),21,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',SrcTblName),2),25,4)) + '-' +
			 LOWER(SUBSTRING(CONVERT(VARCHAR(100),HASHBYTES('MD5',SrcTblName),2),29,4))
		ELSE '0'
	END, 
	'',
	SrcTblLocation,
	SrcTblName, 
	JoinCond, 
	SmpName,
	'',
	'',
	GETDATE(),
	'DEA-PS-WS-Repo-To-CL-Repo.psm1'
FROM #WsToClRepoSchema.WrkVwClMigrTblSmp10
;