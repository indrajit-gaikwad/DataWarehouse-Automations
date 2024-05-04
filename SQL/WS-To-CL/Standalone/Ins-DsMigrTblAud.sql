DELETE FROM ODS.DsMigrTblAud WHERE ClientCd IN (SELECT DISTINCT ClientCd FROM STG.LdMigrTblAud);

INSERT INTO ODS.DsMigrTblAud
(
	ClientCd,
	TblId,
	ColId,
	IsStrictMatch,
	DataType,
	DefaultValue,
	ColDescription,
	HashColumns,
	ColName,
	IsNullable,
	SrcTblId,
	SrcColId,
	ColType,
	ColTransform,
	RowCreatedTs,
	RowCreatedBy,
	RowUpdatedTs,
	RowUpdatedBy
)
SELECT DISTINCT
	Aud.ClientCd,
	Aud.TblId,
	Aud.ColId,
	Aud.IsStrictMatch,
	Aud.DataType,
	Aud.DefaultValue,
	Aud.ColDescription,
	Aud.HashColumns,
	Aud.ColName,
	Aud.IsNullable,
	Aud.SrcTblId,
	Aud.SrcColId,
	Aud.ColType,
	Aud.ColTransform,
	Aud.RowCreatedTs,
	Aud.RowCreatedBy,
	NULL,
	NULL
FROM STG.LdMigrTblAud Aud
;