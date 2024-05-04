DELETE FROM ODS.DsMigrTblTrl WHERE ClientCd IN (SELECT DISTINCT ClientCd FROM STG.LdMigrTblTrl);

INSERT INTO ODS.DsMigrTblTrl
(
	ClientCd,
	TblName,
	OverrideSql,
	TblSchema,
	SqlType,
	ModType,
	TblId,
	RowCreatedTs,
	RowCreatedBy,
	RowUpdatedTs,
	RowUpdatedBy
)
SELECT DISTINCT
	Trl.ClientCd,
	Trl.TblName,
	Trl.OverrideSql,
	Trl.TblSchema,
	Trl.SqlType,
	Trl.ModType,
	Trl.TblId,
	Trl.RowCreatedTs,
	Trl.RowCreatedBy,
	NULL,
	NULL
FROM STG.LdMigrTblTrl Trl
;