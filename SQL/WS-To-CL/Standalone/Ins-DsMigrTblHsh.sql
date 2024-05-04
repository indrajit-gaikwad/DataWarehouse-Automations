DELETE FROM ODS.DsMigrTblHsh WHERE ClientCd IN (SELECT DISTINCT ClientCd FROM STG.LdMigrTblHsh);

INSERT INTO ODS.DsMigrTblHsh
(
	ClientCd,
	TblId,
	ColId,
	IsStrictMatch,
	DataType,
	DefaultValue,
	ColDescription,
	HashAlgorithm,
	HashTblId,
	HashColId,
	ColName,
	IsNullable,
	SrcTblId,
	SrcColId,
	ColTransform,
	RowCreatedTs,
	RowCreatedBy,
	RowUpdatedTs,
	RowUpdatedBy
)
SELECT DISTINCT
	Hsh.ClientCd,
	Hsh.TblId,
	Hsh.ColId,
	Hsh.IsStrictMatch,
	Hsh.DataType,
	Hsh.DefaultValue,
	Hsh.ColDescription,
	Hsh.HashAlgorithm,
	Hsh.HashTblId,
	Hsh.HashColId,
	Hsh.ColName,
	Hsh.IsNullable,
	Hsh.SrcTblId,
	Hsh.SrcColId,
	Hsh.ColTransform,
	Hsh.RowCreatedTs,
	Hsh.RowCreatedBy,
	NULL,
	NULL
FROM STG.LdMigrTblHsh Hsh
;