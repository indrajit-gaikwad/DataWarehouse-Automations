DELETE FROM ODS.DsMigrTblHdr WHERE ClientCd IN (SELECT DISTINCT ClientCd FROM STG.LdMigrTblHdr);

INSERT INTO ODS.DsMigrTblHdr
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
	RowCreatedBy,
	RowUpdatedTs,
	RowUpdatedBy
)
SELECT DISTINCT
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
	Hdr.RowCreatedBy,
	NULL,
	NULL
FROM STG.LdMigrTblHdr Hdr
;	