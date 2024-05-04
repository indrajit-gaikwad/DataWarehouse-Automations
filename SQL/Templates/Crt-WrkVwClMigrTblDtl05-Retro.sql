CREATE OR ALTER VIEW #WsToClRepoSchema.WrkVwClMigrTblDtl05
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
	WsCol.ac_src_strategy,
	CASE
		WHEN WsCol.ac_nulls_flag = 'Y'
		THEN 'true'
		ELSE 'false'
	END,
	'false',
	'false',
	WsCol.ac_src_table,
	WsCol.ac_src_column,	
	CASE
		WHEN WsCol.ac_transform_type = 'D'
		THEN WsCol.ac_transform_code
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
LEFT JOIN #SourceDbSchema.ws_source_mapping_tab WsSmt
  ON WsObj.ObjectKey = WsSmt.smt_parent_obj_key    
WHERE WsSmt.smt_parent_obj_key IS NULL
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
	WsCol.dc_src_strategy,
	CASE
		WHEN WsCol.dc_nulls_flag = 'Y'
		THEN 'true'
		ELSE 'false'
	END,
	'false',
	'false',	
	WsCol.dc_src_table,
	WsCol.dc_src_column,	
	CASE
		WHEN WsCol.dc_transform_type = 'D'
		THEN WsCol.dc_transform_code
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
LEFT JOIN #SourceDbSchema.ws_source_mapping_tab WsSmt
  ON WsObj.ObjectKey = WsSmt.smt_parent_obj_key    
WHERE WsObj.ObjectName NOT IN ('dss_fact_table','dss_source_system','dim_date')
  AND WsSmt.smt_parent_obj_key IS NULL  
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
	WsCol.fc_src_strategy,
	CASE
		WHEN WsCol.fc_nulls_flag = 'Y'
		THEN 'true'
		ELSE 'false'
	END,
	'false',
	'false',	
	WsCol.fc_src_table,
	WsCol.fc_src_column,	
	CASE
		WHEN WsCol.fc_transform_type = 'D'
		THEN WsCol.fc_transform_code
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
LEFT JOIN #SourceDbSchema.ws_source_mapping_tab WsSmt
  ON WsObj.ObjectKey = WsSmt.smt_parent_obj_key    
WHERE WsSmt.smt_parent_obj_key IS NULL  
UNION ALL
-- LOAD
SELECT 
	WsObj.ObjectName,
	WsCol.lc_col_name,
	CASE
		WHEN WsCol.lc_data_type LIKE '%identity%'
		THEN 'true'
		ELSE 'false'
	END,
	WsCol.lc_data_type,
	WsCol.lc_default_value,
	WsCol.lc_src_strategy,
	CASE
		WHEN WsCol.lc_nulls_flag = 'Y'
		THEN 'true'
		ELSE 'false'
	END,
	'false',
	'false',	
	WsCol.lc_src_table,
	WsCol.lc_src_column,	
	CASE
		WHEN WsCol.lc_transform_type IN ('A','D')
		THEN WsCol.lc_transform_code
		ELSE NULL
	END,
	WsObj.ObjectDescription,
	RefAud.AuditColumnName,
	WsCol.lc_order,
	'false',
	'false'		
FROM #SourceDbSchema.ws_load_col WsCol
JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsCol.lc_obj_key = WsObj.ObjectKey
LEFT JOIN #WsToClRepoSchema.vRefAuditColumns RefAud
  ON WsCol.lc_col_name = RefAud.AuditColumnName
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
	WsCol.nc_src_strategy,
	CASE
		WHEN WsCol.nc_nulls_flag = 'Y'
		THEN 'true'
		ELSE 'false'
	END,
	'false',
	'false',	
	WsCol.nc_src_table,
	WsCol.nc_src_column,	
	CASE
		WHEN WsCol.nc_transform_type = 'D'
		THEN WsCol.nc_transform_code
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
LEFT JOIN #SourceDbSchema.ws_source_mapping_tab WsSmt
  ON WsObj.ObjectKey = WsSmt.smt_parent_obj_key    
WHERE WsSmt.smt_parent_obj_key IS NULL  
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
	WsCol.oc_src_strategy,
	CASE
		WHEN WsCol.oc_nulls_flag = 'Y'
		THEN 'true'
		ELSE 'false'
	END,
	'false',
	'false',	
	WsCol.oc_src_table,
	WsCol.oc_src_column,	
	CASE
		WHEN WsCol.oc_transform_type = 'D'
		THEN WsCol.oc_transform_code
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
LEFT JOIN #SourceDbSchema.ws_source_mapping_tab WsSmt
  ON WsObj.ObjectKey = WsSmt.smt_parent_obj_key    
WHERE WsSmt.smt_parent_obj_key IS NULL  
UNION ALL
-- RETRO
SELECT 
	WsObj.ObjectName,
	WsCol.rc_col_name,
	CASE
		WHEN WsCol.rc_data_type LIKE '%identity%'
		THEN 'true'
		ELSE 'false'
	END,
	WsCol.rc_data_type,
	WsCol.rc_default_value,
	WsCol.rc_src_strategy,
	CASE
		WHEN WsCol.rc_nulls_flag = 'Y'
		THEN 'true'
		ELSE 'false'
	END,
	'false',
	'false',	
	'NA',
	'NA',	
	'NA',
	'SOURCE',
	RefAud.AuditColumnName,
	WsCol.rc_order,
	'false',
	'false'
FROM #SourceDbSchema.ws_retro_col WsCol
JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsCol.rc_obj_key = WsObj.ObjectKey
LEFT JOIN #WsToClRepoSchema.vRefAuditColumns RefAud
  ON WsCol.rc_col_name = RefAud.AuditColumnName  
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
	WsCol.sc_src_strategy,
	CASE
		WHEN WsCol.sc_nulls_flag = 'Y'
		THEN 'true'
		ELSE 'false'
	END,
	'false',
	'false',	
	WsCol.sc_src_table,
	WsCol.sc_src_column,	
	CASE
		WHEN WsCol.sc_transform_type = 'D'
		THEN WsCol.sc_transform_code
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
LEFT JOIN #SourceDbSchema.ws_source_mapping_tab WsSmt
  ON WsObj.ObjectKey = WsSmt.smt_parent_obj_key    
WHERE COALESCE(WsCol.sc_key_type,'') NOT IN ('h','l','c')  
  AND WsSmt.smt_parent_obj_key IS NULL
UNION ALL
-- VIEW
SELECT 
	WsObj.ObjectName,
	WsCol.vc_col_name,
	CASE
		WHEN WsCol.vc_data_type LIKE '%identity%'
		THEN 'true'
		ELSE 'false'
	END,
	WsCol.vc_data_type,
	WsCol.vc_default_value,
	WsCol.vc_src_strategy,
	CASE
		WHEN WsCol.vc_nulls_flag = 'Y'
		THEN 'true'
		ELSE 'false'
	END,
	'false',
	'false',	
	WsCol.vc_src_table,
	WsCol.vc_src_column,	
	CASE
		WHEN WsCol.vc_transform_type = 'D'
		THEN WsCol.vc_transform_code
		ELSE NULL
	END,
	WsObj.ObjectDescription,
	RefAud.AuditColumnName,
	WsCol.vc_order,
	'false',
	'false'
FROM #SourceDbSchema.ws_view_col WsCol
JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsCol.vc_obj_key = WsObj.ObjectKey
LEFT JOIN #WsToClRepoSchema.vRefAuditColumns RefAud
  ON WsCol.vc_col_name = RefAud.AuditColumnName
;