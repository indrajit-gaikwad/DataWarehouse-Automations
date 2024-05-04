CREATE OR ALTER VIEW #WsToClRepoSchema.WrkVwClMigrTblHsh05
(
	TblName,
	ColName,
	DataType,
	DefaultValue,
	ColDescription,
	SrcTblName,
	SrcColName,
	Nullable,
	ColKeyType
) AS
SELECT 
	WsTab.st_table_name, 
	WsStg1.sc_col_name,	
	WsStg1.sc_data_type,
	WsStg1.sc_default_value,
	WsStg1.sc_src_strategy, 
	WsStg2.sc_src_table,
	WsStg2.sc_src_column, 
	CASE
		WHEN WsStg1.sc_nulls_flag = 'Y'
		THEN 'true'
		ELSE 'false'
	END,
	WsStg2.sc_key_type
FROM #SourceDbSchema.ws_stage_tab WsTab 
JOIN #SourceDbSchema.ws_column_relationship WsClr
  ON WsTab.st_obj_key = WsClr.cr_obj_key 
JOIN #SourceDbSchema.ws_stage_col WsStg1 
  ON WsClr.cr_obj_key = WsStg1.sc_obj_key 
 AND WsClr.cr_col_key = WsStg1.sc_col_key
JOIN #SourceDbSchema.ws_stage_col WsStg2
  ON WsClr.cr_obj_key = WsStg2.sc_obj_key 
 AND WsClr.cr_rel_col_key = WsStg2.sc_col_key
WHERE WsClr.cr_rel_type = '#'
  AND WsTab.st_type_ind = 'D'
;