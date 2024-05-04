INSERT INTO STG.LdMigrTblSmp
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
	RowCreatedBy
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
	Smp.RowCreatedBy
FROM DE_1000_00.WSCL.WrkVwClMigrTblSmp Smp
;