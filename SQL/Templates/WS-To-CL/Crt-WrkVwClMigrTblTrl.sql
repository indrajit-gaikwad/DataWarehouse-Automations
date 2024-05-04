CREATE OR ALTER VIEW #WsToClRepoSchema.WrkVwClMigrTblTrl
(
	ClientCd,
	TblName,
	OverrideSql,
	TblSchema,
	SqlType,
	ModType,
	TblId,
	RowCreatedTs,
	RowCreatedBy
) AS
SELECT DISTINCT
	'#ClientCode',
	TblName,
	'',
	'',
	WsWrk.TblType,
    'sql',
	TblId,	
	GETDATE(),
	'DEA-PS-WS-Repo-To-CL-Repo.psm1'
FROM #WsToClRepoSchema.WrkVwClMigrTblHdr WsWrk
;