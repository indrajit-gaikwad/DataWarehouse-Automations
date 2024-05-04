INSERT INTO STG.LdMigrTblHdr
(
	ClientCd,
	TblName,
	TblId,
	InsertStrategy,
	IsTestsEnabled,
	IsTruncateBefore,
	TblDatabase,
	IsDeployEnabled,
	TblDescription,
	IsDataVault,
	IsMultiSource,
	LocationName,
	Materialization,
	TblType,
	RowCreatedTs,
	RowCreatedBy
)
SELECT 
	Hdr.ClientCd,
	Hdr.TblName,
	Hdr.TblId,
	Hdr.InsertStrategy,
	Hdr.IsTestsEnabled,
	Hdr.IsTruncateBefore,
	Hdr.TblDatabase,
	Hdr.IsDeployEnabled,
	Hdr.TblDescription,
	Hdr.IsDataVault,
	Hdr.IsMultiSource,
	Hdr.LocationName,
	Hdr.Materialization,
	Hdr.TblType,
	Hdr.RowCreatedTs,
	Hdr.RowCreatedBy
FROM DE_1000_00.WSCL.WrkVwClMigrTblHdr Hdr
;	