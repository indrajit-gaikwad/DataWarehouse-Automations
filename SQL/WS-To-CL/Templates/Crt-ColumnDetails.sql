------------------------------------------------------------------------------------------ 
-- Script	: Crt-ObjectDetails
-- Author	: Indrajit Gaikwad
-- Date  	: 15-AUG-2023
-- Purpose	: Create View - ObjectDetails
------------------------------------------------------------------------------------------- 

CREATE OR ALTER VIEW #WsToClRepoSchema.ColumnDetails
(
	ClientCd,
	ObjectKey,
	ColumnName,
	DataType,
	NullsFlag,
	SrcTable,
	SrcColumn,
	TransformInd,
	TransformCode,
	ColumnDescriptionFlag,
	ColOrder,
	ColOrderRank
) AS
SELECT 
	'#ClientCode',
	ac_obj_key,
	ac_col_name,
	ac_data_type,
	ac_nulls_flag,
	ac_src_table,
	ac_src_column,
	ac_transform_type, 
	ac_transform_code,
	CASE
		WHEN LEN(TRIM(CONVERT(VARCHAR(MAX),ac_src_strategy))) >= 1
		THEN 'true'
		ELSE 'false'
	END,
	ac_order,
	RANK() OVER(PARTITION BY ac_obj_key ORDER BY ac_order)
FROM #SourceDbSchema.ws_agg_col wac
UNION ALL
SELECT 
	'#ClientCode',
	dc_obj_key,
	dc_col_name,
	dc_data_type,
	dc_nulls_flag,
	dc_src_table,
	dc_src_column,
	dc_transform_type,
	dc_transform_code,
	CASE
		WHEN LEN(TRIM(CONVERT(VARCHAR(MAX),dc_src_strategy))) >= 1
		THEN 'true'
		ELSE 'false'
	END,
	dc_order,
	RANK() OVER(PARTITION BY dc_obj_key ORDER BY dc_order)	
FROM #SourceDbSchema.ws_dim_col wdc
UNION ALL
SELECT 
	'#ClientCode',
	fc_obj_key,
	fc_col_name,
	fc_data_type,
	fc_nulls_flag,
	fc_src_table,
	fc_src_column,
	fc_transform_type,
	fc_transform_code,
	CASE
		WHEN LEN(TRIM(CONVERT(VARCHAR(MAX),fc_src_strategy))) >= 1
		THEN 'true'
		ELSE 'false'
	END,
	fc_order,
	RANK() OVER(PARTITION BY fc_obj_key ORDER BY fc_order)	
FROM #SourceDbSchema.ws_fact_col wfc
UNION ALL
SELECT 
	'#ClientCode',
	lc_obj_key,
	lc_col_name,
	lc_data_type,
	lc_nulls_flag,
	lc_src_table,
	lc_src_column,
	lc_transform_type,
	lc_transform_code,
	CASE
		WHEN LEN(TRIM(CONVERT(VARCHAR(MAX),lc_src_strategy))) >= 1
		THEN 'true'
		ELSE 'false'
	END,
	lc_order,
	RANK() OVER(PARTITION BY lc_obj_key ORDER BY lc_order)	
FROM #SourceDbSchema.ws_load_col wlc
UNION ALL
SELECT 
	'#ClientCode',
	nc_obj_key,
	nc_col_name,
	nc_data_type,
	nc_nulls_flag,
	nc_src_table,
	nc_src_column,
	nc_transform_type,
	nc_transform_code,
	CASE
		WHEN LEN(TRIM(CONVERT(VARCHAR(MAX),nc_src_strategy))) >= 1
		THEN 'true'
		ELSE 'false'
	END,
	nc_order,
	RANK() OVER(PARTITION BY nc_obj_key ORDER BY nc_order)	
FROM #SourceDbSchema.ws_normal_col wnc
UNION ALL
SELECT 
	'#ClientCode',
	oc_obj_key,
	oc_col_name,
	oc_data_type,
	oc_nulls_flag,
	oc_src_table,
	oc_src_column,
	oc_transform_type,
	oc_transform_code,
	CASE
		WHEN LEN(TRIM(CONVERT(VARCHAR(MAX),oc_src_strategy))) >= 1
		THEN 'true'
		ELSE 'false'
	END,
	oc_order,
	RANK() OVER(PARTITION BY oc_obj_key ORDER BY oc_order)	
FROM #SourceDbSchema.ws_ods_col woc
UNION ALL
SELECT 
	'#ClientCode',
	rc_obj_key,
	rc_col_name,
	rc_data_type,
	rc_nulls_flag,
	rc_src_table,
	rc_src_column,
	rc_transform_type,
	rc_transform_code,
	CASE
		WHEN LEN(TRIM(CONVERT(VARCHAR(MAX),rc_src_strategy))) >= 1
		THEN 'true'
		ELSE 'false'
	END,
	rc_order,
	RANK() OVER(PARTITION BY rc_obj_key ORDER BY rc_order)	
FROM #SourceDbSchema.ws_retro_col wrc
UNION ALL
SELECT 
	'#ClientCode',
	sc_obj_key,
	sc_col_name,
	sc_data_type,
	sc_nulls_flag,
	sc_src_table,
	sc_src_column,
	sc_transform_type,
	sc_transform_code,
	CASE
		WHEN LEN(TRIM(CONVERT(VARCHAR(MAX),sc_src_strategy))) >= 1
		THEN 'true'
		ELSE 'false'
	END,
	sc_order,
	RANK() OVER(PARTITION BY sc_obj_key ORDER BY sc_order)
FROM #SourceDbSchema.ws_stage_col wsc
UNION ALL
SELECT 
	'#ClientCode',
	vc_obj_key,
	vc_col_name,
	vc_data_type,
	vc_nulls_flag,
	vc_src_table,
	vc_src_column,
	vc_transform_type,
	vc_transform_code,
	CASE
		WHEN LEN(TRIM(CONVERT(VARCHAR(MAX),vc_src_strategy))) >= 1
		THEN 'true'
		ELSE 'false'
	END,
	vc_order,
	RANK() OVER(PARTITION BY vc_obj_key ORDER BY vc_order)	
FROM #SourceDbSchema.ws_view_col wvc
;