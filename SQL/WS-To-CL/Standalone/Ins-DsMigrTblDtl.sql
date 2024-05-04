DELETE FROM ODS.DsMigrTblDtl WHERE ClientCd IN (SELECT DISTINCT ClientCd FROM STG.LdMigrTblDtl);

INSERT INTO ODS.DsMigrTblDtl 
(
	ClientCd,
	TblId,
	ColId,
	IsIncremental,
	IsStrictMatch,
	DataType,
	DefaultValue,
	ColDescription,
	ColName,
	IsNullable,
	IsPrimaryKey,
	IsUniqueKey,
	SrcTblId,
	SrcColId,
	ColTransform,
	TblType,
	ColOrder,
	IsBusinessKey,
	IsChangeDetection,
	RowCreatedTs,
	RowCreatedBy,
	RowUpdatedTs,
	RowUpdatedBy
)
SELECT DISTINCT
	Dtl.ClientCd,
	Dtl.TblId,
	Dtl.ColId,
	Dtl.IsIncremental,
	Dtl.IsStrictMatch,
	Dtl.DataType,
	Dtl.DefaultValue,
	Dtl.ColDescription,
	Dtl.ColName,
	Dtl.IsNullable,
	Dtl.IsPrimaryKey,
	Dtl.IsUniqueKey,	
	Dtl.SrcTblId,
	Dtl.SrcColId,
	Dtl.ColTransform,
	Dtl.TblType,
	Dtl.ColOrder,
	Dtl.IsBusinessKey,
	Dtl.IsChangeDetection,
	Dtl.RowCreatedTs,
	Dtl.RowCreatedBy,
	NULL,
	NULL
FROM STG.LdMigrTblDtl Dtl
;