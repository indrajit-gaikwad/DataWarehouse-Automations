CREATE OR ALTER VIEW #WsToClRepoSchema.WrkVwClMigrTblAud10
(
	TblName,
	ColName,
	DataType,
	DefaultValue,
	ColDescription,
	Nullable,
	SrcTblName,
	SrcColName,	
	TblType,
	ColType
) AS
-- AGGREGATE
SELECT 
	WsObj.ObjectName,
	WsCol.ac_col_name,
	WsCol.ac_data_type,
	WsCol.ac_default_value,
	WsSmc.smc_src_strategy,
	CASE
		WHEN WsCol.ac_nulls_flag = 'Y'
		THEN 'true'
		ELSE 'false'
	END,
	WsSmc.smc_src_table,
	WsSmc.smc_src_column,	
	'Aggregate',
	RefAud.AuditColumnName
FROM #SourceDbSchema.ws_agg_col WsCol
JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsCol.ac_obj_key = WsObj.ObjectKey
JOIN #WsToClRepoSchema.vRefAuditColumns RefAud
  ON WsCol.ac_col_name = RefAud.AuditColumnName
JOIN #SourceDbSchema.ws_source_mapping_tab WsSmt
  ON WsObj.ObjectKey = WsSmt.smt_parent_obj_key
JOIN #SourceDbSchema.ws_source_mapping_col WsSmc
  ON WsSmt.smt_source_mapping_key = WsSmc.smc_source_mapping_key
 AND WsCol.ac_col_key             = WsSmc.smc_parent_col_key 
UNION ALL
-- DIMENSION
SELECT 
	WsObj.ObjectName,
	WsCol.dc_col_name,
	WsCol.dc_data_type,
	WsCol.dc_default_value,
	WsSmc.smc_src_strategy,
	CASE
		WHEN WsCol.dc_nulls_flag = 'Y'
		THEN 'true'
		ELSE 'false'
	END,
	WsSmc.smc_src_table,
	WsSmc.smc_src_column,	
	'Dimension',
	RefAud.AuditColumnName
FROM #SourceDbSchema.ws_dim_col WsCol
JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsCol.dc_obj_key = WsObj.ObjectKey
JOIN #WsToClRepoSchema.vRefAuditColumns RefAud
  ON WsCol.dc_col_name = RefAud.AuditColumnName
JOIN #SourceDbSchema.ws_source_mapping_tab WsSmt
  ON WsObj.ObjectKey = WsSmt.smt_parent_obj_key
JOIN #SourceDbSchema.ws_source_mapping_col WsSmc
  ON WsSmt.smt_source_mapping_key = WsSmc.smc_source_mapping_key
 AND WsCol.dc_col_key             = WsSmc.smc_parent_col_key  
UNION ALL
-- FACT
SELECT 
	WsObj.ObjectName,
	WsCol.fc_col_name,
	WsCol.fc_data_type,
	WsCol.fc_default_value,
	WsSmc.smc_src_strategy,
	CASE
		WHEN WsCol.fc_nulls_flag = 'Y'
		THEN 'true'
		ELSE 'false'
	END,
	WsSmc.smc_src_table,
	WsSmc.smc_src_column,	
	'Fact',
	RefAud.AuditColumnName
FROM #SourceDbSchema.ws_fact_col WsCol
JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsCol.fc_obj_key = WsObj.ObjectKey
JOIN #WsToClRepoSchema.vRefAuditColumns RefAud
  ON WsCol.fc_col_name = RefAud.AuditColumnName
JOIN #SourceDbSchema.ws_source_mapping_tab WsSmt
  ON WsObj.ObjectKey = WsSmt.smt_parent_obj_key
JOIN #SourceDbSchema.ws_source_mapping_col WsSmc
  ON WsSmt.smt_source_mapping_key = WsSmc.smc_source_mapping_key
 AND WsCol.fc_col_key             = WsSmc.smc_parent_col_key  
UNION ALL
-- EDW 3NF
SELECT 
	WsObj.ObjectName,
	WsCol.nc_col_name,
	WsCol.nc_data_type,
	WsCol.nc_default_value,
	WsSmc.smc_src_strategy,
	CASE
		WHEN WsCol.nc_nulls_flag = 'Y'
		THEN 'true'
		ELSE 'false'
	END,
	WsSmc.smc_src_table,
	WsSmc.smc_src_column,	
	'EDW 3NF',
	RefAud.AuditColumnName
FROM #SourceDbSchema.ws_normal_col WsCol
JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsCol.nc_obj_key = WsObj.ObjectKey
JOIN #WsToClRepoSchema.vRefAuditColumns RefAud
  ON WsCol.nc_col_name = RefAud.AuditColumnName
JOIN #SourceDbSchema.ws_source_mapping_tab WsSmt
  ON WsObj.ObjectKey = WsSmt.smt_parent_obj_key
JOIN #SourceDbSchema.ws_source_mapping_col WsSmc
  ON WsSmt.smt_source_mapping_key = WsSmc.smc_source_mapping_key
 AND WsCol.nc_col_key             = WsSmc.smc_parent_col_key  
UNION ALL
-- DATA STORE
SELECT 
	WsObj.ObjectName,
	WsCol.oc_col_name,
	WsCol.oc_data_type,
	WsCol.oc_default_value,
	WsSmc.smc_src_strategy,
	CASE
		WHEN WsCol.oc_nulls_flag = 'Y'
		THEN 'true'
		ELSE 'false'
	END,
	WsSmc.smc_src_table,
	WsSmc.smc_src_column,	
	'Data Store',
	RefAud.AuditColumnName
FROM #SourceDbSchema.ws_ods_col WsCol
JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsCol.oc_obj_key = WsObj.ObjectKey
JOIN #WsToClRepoSchema.vRefAuditColumns RefAud
  ON WsCol.oc_col_name = RefAud.AuditColumnName
JOIN #SourceDbSchema.ws_source_mapping_tab WsSmt
  ON WsObj.ObjectKey = WsSmt.smt_parent_obj_key
JOIN #SourceDbSchema.ws_source_mapping_col WsSmc
  ON WsSmt.smt_source_mapping_key = WsSmc.smc_source_mapping_key
 AND WsCol.oc_col_key             = WsSmc.smc_parent_col_key  
UNION ALL
-- STAGE
SELECT 
	WsObj.ObjectName,
	WsCol.sc_col_name,
	WsCol.sc_data_type,
	WsCol.sc_default_value,
	WsSmc.smc_src_strategy,
	CASE
		WHEN WsCol.sc_nulls_flag = 'Y'
		THEN 'true'
		ELSE 'false'
	END,
	WsSmc.smc_src_table,
	WsSmc.smc_src_column,	
	'Stage',
	RefAud.AuditColumnName
FROM #SourceDbSchema.ws_stage_col WsCol
JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsCol.sc_obj_key = WsObj.ObjectKey
JOIN #WsToClRepoSchema.vRefAuditColumns RefAud
  ON WsCol.sc_col_name = RefAud.AuditColumnName
JOIN #SourceDbSchema.ws_source_mapping_tab WsSmt
  ON WsObj.ObjectKey = WsSmt.smt_parent_obj_key
JOIN #SourceDbSchema.ws_source_mapping_col WsSmc
  ON WsSmt.smt_source_mapping_key = WsSmc.smc_source_mapping_key
 AND WsCol.sc_col_key             = WsSmc.smc_parent_col_key  
WHERE COALESCE(WsCol.sc_key_type,'') NOT IN ('h','l','c')
;