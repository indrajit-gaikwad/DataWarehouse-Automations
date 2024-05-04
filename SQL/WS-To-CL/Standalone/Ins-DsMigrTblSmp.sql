DELETE FROM ODS.DsMigrTblSmp WHERE ClientCd IN (SELECT DISTINCT ClientCd FROM STG.LdMigrTblSmp);

INSERT INTO ODS.DsMigrTblSmp
(
	ClientCd,
	TblId,
	TblName,
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
	RowCreatedBy,
	RowUpdatedTs,
	RowUpdatedBy
)
SELECT
	Smp.ClientCd,
	Smp.TblId,
	Smp.TblName,
	Smp.SrcTblName,
	Smp.SrcTblId,
	Smp.CustomSql,
	Smp.DepLocName,
	Smp.DepNodeName,
	Smp.JoinCond,
	Smp.SmpName,
	Smp.NoLinkLocName,
	Smp.NoLinkNodeName,
	Smp.RowCreatedTs,
	Smp.RowCreatedBy,
	NULL,
	NULL
FROM STG.LdMigrTblSmp Smp
;