------------------------------------------------------------------------------------------ 
-- Script	: Crt-ObjectDetails
-- Author	: Indrajit Gaikwad
-- Date  	: 15-AUG-2023
-- Purpose	: Create View - ObjectDetails
------------------------------------------------------------------------------------------- 

CREATE OR ALTER VIEW #WsToClRepoSchema.ObjectDetails 
(
	ClientCd,
	ObjectKey, 
	ObjectName,
	ObjectDescription, 
	TargetName, 
	TargetDb, 
	TargetSchema,
	TblDescription
) AS
SELECT
	'#ClientCode',
	oo_obj_key,
	oo_name,
	wob.ot_description,
	dbt.dt_name,
	dbt.dt_database,
	dbt.dt_schema,
	COALESCE(wat.at_description, wdt.dt_description, wft.ft_description, wlt.lt_description, wnt.nt_description, wot.ot_description, wrt.rt_description, wst.st_description, wvt.vt_description)
FROM #SourceDbSchema.ws_obj_object woo
JOIN #SourceDbSchema.ws_obj_type wob
  ON oo_type_key = ot_type_key
LEFT JOIN #SourceDbSchema.ws_dbc_target dbt
  ON oo_target_key = dt_target_key
LEFT JOIN #SourceDbSchema.ws_agg_tab wat
  ON oo_obj_key = at_obj_key
LEFT JOIN #SourceDbSchema.ws_dim_tab wdt
  ON oo_obj_key = dt_obj_key
LEFT JOIN #SourceDbSchema.ws_fact_tab wft
  ON oo_obj_key = ft_obj_key
LEFT JOIN #SourceDbSchema.ws_load_tab wlt
  ON oo_obj_key = lt_obj_key
LEFT JOIN #SourceDbSchema.ws_normal_tab wnt
  ON oo_obj_key = nt_obj_key
LEFT JOIN #SourceDbSchema.ws_ods_tab wot
  ON oo_obj_key = ot_obj_key
LEFT JOIN #SourceDbSchema.ws_retro_tab wrt
  ON oo_obj_key = rt_obj_key
LEFT JOIN #SourceDbSchema.ws_stage_tab wst
  ON oo_obj_key = st_obj_key
LEFT JOIN #SourceDbSchema.ws_view_tab wvt
  ON oo_obj_key = vt_obj_key  
WHERE oo_name NOT IN ('dss_fact_table','dss_source_system')
;