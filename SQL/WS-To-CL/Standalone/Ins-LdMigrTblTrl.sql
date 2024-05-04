INSERT INTO STG.LdMigrTblTrl
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
)
SELECT 
	Trl.ClientCd,
	Trl.TblName,
	Trl.OverrideSql,
	Trl.TblSchema,
	Trl.SqlType,
	Trl.ModType,
	Trl.TblId,
	Trl.RowCreatedTs,
	Trl.RowCreatedBy
FROM DE_1000_00.WSCL.WrkVwClMigrTblTrl Trl
;