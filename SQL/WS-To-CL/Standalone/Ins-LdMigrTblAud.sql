INSERT INTO STG.LdMigrTblAud
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
	RowCreatedBy
)
SELECT 
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
	Aud.RowCreatedBy
FROM DE_1000_00.WSCL.WrkVwClMigrTblAud Aud
;