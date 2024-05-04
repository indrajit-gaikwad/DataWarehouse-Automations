CREATE OR ALTER VIEW #WsToClRepoSchema.WrkVwClMigrTblSmp05
(
	TblName,
	SrcTblName,
	SrcTblLocation,
	JoinCond,
	SmpName
) AS
-- AGGREGATE
SELECT DISTINCT
	WsTab.at_table_name,
	WsCol.ac_src_table,
	COALESCE(WsObj.TargetName,'TGT_PH'),
	COALESCE(REPLACE(REPLACE(REPLACE(CONVERT(varchar(max),WsTab.at_where),'[TABLEOWNER].[','{{ ref('''+ COALESCE(WsObj.TargetName,'TGT_PH') + ''','''),']',''') }}'),'~',' '),'FROM {{ ref('''+ COALESCE(WsObj.TargetName,'TGT_PH') + ''',' + WsTab.at_table_name + ''') }}'),
	WsTab.at_table_name
FROM #SourceDbSchema.ws_agg_tab WsTab
JOIN #SourceDbSchema.ws_agg_col WsCol
  ON WsTab.at_obj_key = WsCol.ac_obj_key
LEFT JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsCol.ac_src_table = WsObj.ObjectName
LEFT JOIN #SourceDbSchema.ws_source_mapping_tab WsSmp
  ON WsTab.at_obj_key = WsSmp.smt_parent_obj_key
WHERE LEN(WsCol.ac_src_table) > 1
  AND WsSmp.smt_parent_obj_key IS NULL
UNION ALL
-- DIMENSION
SELECT DISTINCT
	WsTab.dt_table_name,
	WsCol.dc_src_table,
	COALESCE(WsObj.TargetName,'TGT_PH'),
	COALESCE(REPLACE(REPLACE(REPLACE(CONVERT(varchar(max),WsTab.dt_where),'[TABLEOWNER].[','{{ ref('''+ COALESCE(WsObj.TargetName,'TGT_PH') + ''','''),']',''') }}'),'~',' '),'FROM {{ ref('''+ COALESCE(WsObj.TargetName,'TGT_PH') + ''',' + WsTab.dt_table_name + ''') }}'),
	WsTab.dt_table_name
FROM #SourceDbSchema.ws_dim_tab WsTab
JOIN #SourceDbSchema.ws_dim_col WsCol
  ON WsTab.dt_obj_key = WsCol.dc_obj_key
LEFT JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsCol.dc_src_table = WsObj.ObjectName
LEFT JOIN #SourceDbSchema.ws_source_mapping_tab WsSmp
  ON WsTab.dt_obj_key = WsSmp.smt_parent_obj_key
WHERE LEN(WsCol.dc_src_table) > 1
  AND WsSmp.smt_parent_obj_key IS NULL
  AND WsTab.dt_table_name NOT IN ('dim_date','dss_fact_table','dss_source_system')
UNION ALL
-- FACT
SELECT DISTINCT
	WsTab.ft_table_name,
	WsCol.fc_src_table,
	COALESCE(WsObj.TargetName,'TGT_PH'),
	COALESCE(REPLACE(REPLACE(REPLACE(CONVERT(varchar(max),WsTab.ft_where),'[TABLEOWNER].[','{{ ref('''+ COALESCE(WsObj.TargetName,'TGT_PH') + ''','''),']',''') }}'),'~',' '),'FROM {{ ref('''+ COALESCE(WsObj.TargetName,'TGT_PH') + ''',' + WsTab.ft_table_name + ''') }}'),
	WsTab.ft_table_name
FROM #SourceDbSchema.ws_fact_tab WsTab
JOIN #SourceDbSchema.ws_fact_col WsCol
  ON WsTab.ft_obj_key = WsCol.fc_obj_key
JOIN #WsToClRepoSchema.ObjectDetails WsObd
  ON WsTab.ft_obj_key = WsObd.ObjectKey  
LEFT JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsCol.fc_src_table = WsObj.ObjectName
LEFT JOIN #SourceDbSchema.ws_source_mapping_tab WsSmp
  ON WsTab.ft_obj_key = WsSmp.smt_parent_obj_key  
WHERE LEN(WsCol.fc_src_table) > 1
  AND WsSmp.smt_parent_obj_key IS NULL
UNION ALL
-- LOAD
SELECT DISTINCT
	WsTab.lt_table_name,
	'NA',
	COALESCE(WsObj.TargetName,'TGT_PH'), 
	REPLACE('FROM {{ ref(''' + COALESCE(WsObj.TargetName,'TGT_PH') + ''', ''' + WsTab.lt_table_name + ''') }}','~',' '),
	WsTab.lt_table_name
FROM #SourceDbSchema.ws_load_tab WsTab
JOIN #SourceDbSchema.ws_load_col WsCol
  ON WsTab.lt_obj_key = WsCol.lc_obj_key
JOIN #WsToClRepoSchema.ObjectDetails WsObd
  ON WsTab.lt_obj_key = WsObd.ObjectKey 
LEFT JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsTab.lt_table_name = WsObj.ObjectName
WHERE LEN(WsCol.lc_src_table) > 1
UNION ALL
-- EDW 3NF
SELECT DISTINCT
	WsTab.nt_table_name,
	WsCol.nc_src_table,
	COALESCE(WsObj.TargetName,'TGT_PH'),
	COALESCE(REPLACE(REPLACE(REPLACE(CONVERT(varchar(max),WsTab.nt_where),'[TABLEOWNER].[','{{ ref('''+ COALESCE(WsObj.TargetName,'TGT_PH') + ''','''),']',''') }}'),'~',' '),'FROM {{ ref('''+ COALESCE(WsObj.TargetName,'TGT_PH') + ''',''' + WsTab.nt_table_name + ''') }}'),
	WsTab.nt_table_name
FROM #SourceDbSchema.ws_normal_tab WsTab
JOIN #SourceDbSchema.ws_normal_col WsCol
  ON WsTab.nt_obj_key = WsCol.nc_obj_key
JOIN #WsToClRepoSchema.ObjectDetails WsObd
  ON WsTab.nt_obj_key = WsObd.ObjectKey 
LEFT JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsCol.nc_src_table = WsObj.ObjectName
LEFT JOIN #SourceDbSchema.ws_source_mapping_tab WsSmp
  ON WsTab.nt_obj_key = WsSmp.smt_parent_obj_key  
WHERE LEN(WsCol.nc_src_table) > 1
  AND WsSmp.smt_parent_obj_key IS NULL
UNION ALL
-- DATA STORE
SELECT DISTINCT
	WsTab.ot_table_name,
	WsCol.oc_src_table,
	COALESCE(WsObj.TargetName,'TGT_PH'),
	COALESCE(REPLACE(REPLACE(REPLACE(CONVERT(varchar(max),WsTab.ot_where),'[TABLEOWNER].[','{{ ref('''+ COALESCE(WsObj.TargetName,'TGT_PH') + ''','''),']',''') }}'),'~',' '),'FROM {{ ref('''+ COALESCE(WsObj.TargetName,'TGT_PH') + ''',' + WsTab.ot_table_name + ''') }}'),
	WsTab.ot_table_name
FROM #SourceDbSchema.ws_ods_tab WsTab
JOIN #SourceDbSchema.ws_ods_col WsCol
  ON WsTab.ot_obj_key = WsCol.oc_obj_key
JOIN #WsToClRepoSchema.ObjectDetails WsObd
  ON WsTab.ot_obj_key = WsObd.ObjectKey 
LEFT JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsCol.oc_src_table = WsObj.ObjectName
LEFT JOIN #SourceDbSchema.ws_source_mapping_tab WsSmp
  ON WsTab.ot_obj_key = WsSmp.smt_parent_obj_key
WHERE LEN(WsCol.oc_src_table) > 1
  AND WsSmp.smt_parent_obj_key IS NULL
UNION ALL
-- RETRO
SELECT DISTINCT
	WsTab.rt_table_name,
	'NA',
	COALESCE(WsObj.TargetName,'TGT_PH'), 
	REPLACE('FROM {{ ref(''' + COALESCE(WsObj.TargetName,'TGT_PH') + ''', ''' + WsTab.rt_table_name + ''') }}','~',' '),
	WsTab.rt_table_name
FROM #SourceDbSchema.ws_retro_tab WsTab
JOIN #SourceDbSchema.ws_retro_col WsCol
  ON WsTab.rt_obj_key = WsCol.rc_obj_key
JOIN #WsToClRepoSchema.ObjectDetails WsObd
  ON WsTab.rt_obj_key = WsObd.ObjectKey 
LEFT JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsCol.rc_src_table = WsObj.ObjectName 
WHERE LEN(WsCol.rc_src_table) > 1
UNION ALL
-- STAGE
SELECT DISTINCT
	WsTab.st_table_name,
	WsCol.sc_src_table,
	COALESCE(WsObj.TargetName,'TGT_PH'),
	COALESCE(REPLACE(REPLACE(REPLACE(CONVERT(varchar(max),WsTab.st_where),'[TABLEOWNER].[','{{ ref('''+ COALESCE(WsObj.TargetName,'TGT_PH') + ''','''),']',''') }}'),'~',' '),'FROM {{ ref('''+ COALESCE(WsObj.TargetName,'TGT_PH') + ''',' + WsTab.st_table_name + ''') }}'),
	WsTab.st_table_name
FROM #SourceDbSchema.ws_stage_tab WsTab
JOIN #SourceDbSchema.ws_stage_col WsCol
  ON WsTab.st_obj_key = WsCol.sc_obj_key
LEFT JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsCol.sc_src_table = WsObj.ObjectName
LEFT JOIN #SourceDbSchema.ws_source_mapping_tab WsSmp
  ON WsTab.st_obj_key = WsSmp.smt_parent_obj_key
WHERE LEN(WsCol.sc_src_table) > 1
  AND WsSmp.smt_parent_obj_key IS NULL
UNION ALL
-- VIEW
SELECT DISTINCT
	WsTab.vt_table_name,
	WsCol.vc_src_table,
	COALESCE(WsObj.TargetName,'TGT_PH'), 
	COALESCE(REPLACE(REPLACE(REPLACE(CONVERT(varchar(max),WsTab.vt_view_where),'[TABLEOWNER].[','{{ ref('''+ COALESCE(WsObj.TargetName,'TGT_PH') + ''','''),']',''') }}'),'~',' '),'FROM {{ ref('''+ COALESCE(WsObj.TargetName,'TGT_PH') + ''',' + WsTab.vt_table_name + ''') }}'),
	WsTab.vt_table_name
FROM #SourceDbSchema.ws_view_tab WsTab
JOIN #SourceDbSchema.ws_view_col WsCol
  ON WsTab.vt_obj_key = WsCol.vc_obj_key
JOIN #WsToClRepoSchema.ObjectDetails WsObd
  ON WsTab.vt_obj_key = WsObd.ObjectKey 
LEFT JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsCol.vc_src_table = WsObj.ObjectName 
WHERE LEN(WsCol.vc_src_table) > 1
;