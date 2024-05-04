CREATE OR ALTER VIEW #WsToClRepoSchema.WrkVwClMigrTblDtl10
(
	TblName,
	ColName,
	IsIncremental,
	DataType,
	DefaultValue,
	ColDescription,
	Nullable,
	PrimaryKey,
	UniqueKey,
	SrcTblName,
	SrcColName,	
	ColTransform,
	TblType,
	ColType,
	ColOrder,
	IsBusinessKey,
	IsChangeDetection
) AS
-- AGGREGATE
SELECT 
	WsObj.ObjectName,
	WsCol.ac_col_name,
	CASE
		WHEN WsCol.ac_data_type LIKE '%identity%'
		THEN 'true'
		ELSE 'false'
	END,
	WsCol.ac_data_type,
	WsCol.ac_default_value,
	WsSmc.smc_src_strategy,
	CASE
		WHEN WsCol.ac_nulls_flag = 'Y'
		THEN 'true'
		ELSE 'false'
	END,
	'false',
	'false',
	WsSmc.smc_src_table,
	WsSmc.smc_src_column,	
	CASE
		WHEN WsSmc.smc_transform_type = 'D'
		THEN WsSmc.smc_transform_code
		ELSE NULL
	END,
	WsObj.ObjectDescription,
	RefAud.AuditColumnName,
	WsCol.ac_order,
	CASE
		WHEN WsCol.ac_key_type = 'A'
		THEN 'true'
		ELSE 'false'
	END,
	CASE
		WHEN WsCol.ac_key_type = '7'
		THEN 'true'
		ELSE 'false'
	END	
FROM #SourceDbSchema.ws_agg_col WsCol
JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsCol.ac_obj_key = WsObj.ObjectKey
LEFT JOIN #WsToClRepoSchema.vRefAuditColumns RefAud
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
	CASE
		WHEN WsCol.dc_data_type LIKE '%identity%'
		THEN 'true'
		ELSE 'false'
	END,
	WsCol.dc_data_type,
	WsCol.dc_default_value,
	WsSmc.smc_src_strategy,
	CASE
		WHEN WsCol.dc_nulls_flag = 'Y'
		THEN 'true'
		ELSE 'false'
	END,
	'false',
	'false',	
	WsSmc.smc_src_table,
	WsSmc.smc_src_column,	
	CASE
		WHEN WsSmc.smc_transform_type = 'D'
		THEN WsSmc.smc_transform_code
		ELSE NULL
	END,
	WsObj.ObjectDescription,
	RefAud.AuditColumnName,
	WsCol.dc_order,
	CASE
		WHEN WsCol.dc_key_type = 'A'
		THEN 'true'
		ELSE 'false'
	END,
	CASE
		WHEN WsCol.dc_key_type = '7'
		THEN 'true'
		ELSE 'false'
	END	
FROM #SourceDbSchema.ws_dim_col WsCol
JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsCol.dc_obj_key = WsObj.ObjectKey
LEFT JOIN #WsToClRepoSchema.vRefAuditColumns RefAud
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
	CASE
		WHEN WsCol.fc_data_type LIKE '%identity%'
		THEN 'true'
		ELSE 'false'
	END,
	WsCol.fc_data_type,
	WsCol.fc_default_value,
	WsSmc.smc_src_strategy,
	CASE
		WHEN WsCol.fc_nulls_flag = 'Y'
		THEN 'true'
		ELSE 'false'
	END,
	'false',
	'false',	
	WsSmc.smc_src_table,
	WsSmc.smc_src_column,	
	CASE
		WHEN WsSmc.smc_transform_type = 'D'
		THEN WsSmc.smc_transform_code
		ELSE NULL
	END,
	WsObj.ObjectDescription,
	RefAud.AuditColumnName,
	WsCol.fc_order,
	CASE
		WHEN WsCol.fc_key_type = 'A'
		THEN 'true'
		ELSE 'false'
	END,
	CASE
		WHEN WsCol.fc_key_type = '7'
		THEN 'true'
		ELSE 'false'
	END	
FROM #SourceDbSchema.ws_fact_col WsCol
JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsCol.fc_obj_key = WsObj.ObjectKey
LEFT JOIN #WsToClRepoSchema.vRefAuditColumns RefAud
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
	CASE
		WHEN WsCol.nc_data_type LIKE '%identity%'
		THEN 'true'
		ELSE 'false'
	END,
	WsCol.nc_data_type,
	WsCol.nc_default_value,
	WsSmc.smc_src_strategy,
	CASE
		WHEN WsCol.nc_nulls_flag = 'Y'
		THEN 'true'
		ELSE 'false'
	END,
	'false',
	'false',	
	WsSmc.smc_src_table,
	WsSmc.smc_src_column,	
	CASE
		WHEN WsSmc.smc_transform_type = 'D'
		THEN WsSmc.smc_transform_code
		ELSE NULL
	END,
	WsObj.ObjectDescription,
	RefAud.AuditColumnName,
	WsCol.nc_order,
	CASE
		WHEN WsCol.nc_key_type = 'A'
		THEN 'true'
		ELSE 'false'
	END,
	CASE
		WHEN WsCol.nc_key_type = '7'
		THEN 'true'
		ELSE 'false'
	END	
FROM #SourceDbSchema.ws_normal_col WsCol
JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsCol.nc_obj_key = WsObj.ObjectKey
LEFT JOIN #WsToClRepoSchema.vRefAuditColumns RefAud
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
	CASE
		WHEN WsCol.oc_data_type LIKE '%identity%'
		THEN 'true'
		ELSE 'false'
	END,
	WsCol.oc_data_type,
	WsCol.oc_default_value,
	WsSmc.smc_src_strategy,
	CASE
		WHEN WsCol.oc_nulls_flag = 'Y'
		THEN 'true'
		ELSE 'false'
	END,
	'false',
	'false',	
	WsSmc.smc_src_table,
	WsSmc.smc_src_column,	
	CASE
		WHEN WsSmc.smc_transform_type = 'D'
		THEN WsSmc.smc_transform_code
		ELSE NULL
	END,
	WsObj.ObjectDescription,
	RefAud.AuditColumnName,
	WsCol.oc_order,
	CASE
		WHEN WsCol.oc_key_type = 'A'
		THEN 'true'
		ELSE 'false'
	END,
	CASE
		WHEN WsCol.oc_key_type = '7'
		THEN 'true'
		ELSE 'false'
	END	
FROM #SourceDbSchema.ws_ods_col WsCol
JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsCol.oc_obj_key = WsObj.ObjectKey
LEFT JOIN #WsToClRepoSchema.vRefAuditColumns RefAud
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
	CASE
		WHEN WsCol.sc_data_type LIKE '%identity%'
		THEN 'true'
		ELSE 'false'
	END,
	WsCol.sc_data_type,
	WsCol.sc_default_value,
	WsSmc.smc_src_strategy,
	CASE
		WHEN WsCol.sc_nulls_flag = 'Y'
		THEN 'true'
		ELSE 'false'
	END,
	'false',
	'false',	
	WsSmc.smc_src_table,
	WsSmc.smc_src_column,	
	CASE
		WHEN WsSmc.smc_transform_type = 'D'
		THEN WsSmc.smc_transform_code
		ELSE NULL
	END,
	WsObj.ObjectDescription,
	RefAud.AuditColumnName,
	WsCol.sc_order,
	CASE
		WHEN WsCol.sc_key_type = 'A'
		THEN 'true'
		ELSE 'false'
	END,
	CASE
		WHEN WsCol.sc_key_type = '7'
		THEN 'true'
		ELSE 'false'
	END	
FROM #SourceDbSchema.ws_stage_col WsCol
JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsCol.sc_obj_key = WsObj.ObjectKey
LEFT JOIN #WsToClRepoSchema.vRefAuditColumns RefAud
  ON WsCol.sc_col_name = RefAud.AuditColumnName
JOIN #SourceDbSchema.ws_source_mapping_tab WsSmt
  ON WsObj.ObjectKey = WsSmt.smt_parent_obj_key
JOIN #SourceDbSchema.ws_source_mapping_col WsSmc
  ON WsSmt.smt_source_mapping_key = WsSmc.smc_source_mapping_key
 AND WsCol.sc_col_key             = WsSmc.smc_parent_col_key  
WHERE COALESCE(WsCol.sc_key_type,'') NOT IN ('h','l','c')
;