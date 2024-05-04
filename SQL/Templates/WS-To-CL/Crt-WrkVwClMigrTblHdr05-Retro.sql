CREATE OR ALTER VIEW #WsToClRepoSchema.WrkVwClMigrTblHdr05 (TblName, InsertStrategy, TruncateBefore, TargetDb, TblDescription, IsDataVault, IsMultiSource, LocationName, Materialization, ObjectType, Rnk) AS 
-- AGGREGATE (NORMAL)
SELECT 
	at_table_name AS TblName,
	COALESCE(WsTem.InsertStrategy,'Merge') AS InsertStrategy,
	COALESCE(WsTem.TruncateBefore,'false') AS TruncateBefore,
	COALESCE(WsObj.TargetDb,'PH_DATABASE') AS TargetDb,
	WsTab.at_description AS TblDescription,
	'false' AS IsDataVault,
	'false' AS IsMultiSource,
	COALESCE(WsObj.TargetName,'TGT_PH') AS LocationName,
	'Table' AS Materialization,
	WsObj.ObjectDescription AS ObjectType,
	RANK() OVER (PARTITION BY at_table_name ORDER BY WsTem.TemplateName DESC) AS Rnk
FROM #SourceDbSchema.ws_agg_tab WsTab
LEFT JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsTab.at_obj_key = WsObj.ObjectKey
LEFT JOIN #WsToClRepoSchema.vObjectTemplates WsTem
  ON WsTab.at_obj_key = WsTem.ObjectKey
LEFT JOIN #SourceDbSchema.ws_scr_header WsUpd
  ON WsTab.at_update_key = WsUpd.sh_obj_key
LEFT JOIN #SourceDbSchema.ws_scr_header WsCst
  ON WsTab.at_build_key = WsCst.sh_obj_key
LEFT JOIN #SourceDbSchema.ws_source_mapping_tab WsSmt
  ON WsTab.at_obj_key = WsSmt.smt_parent_obj_key
WHERE WsSmt.smt_parent_obj_key IS NULL  
UNION ALL  
-- DIMENSION (NORMAL)
SELECT 
	dt_table_name AS TblName,
	COALESCE(WsTem.InsertStrategy,'Merge') AS InsertStrategy,
	COALESCE(WsTem.TruncateBefore,'false') AS TruncateBefore,
	COALESCE(WsObj.TargetDb,'SOURCE_DB') AS TargetDb,
	WsTab.dt_description AS TblDescription,
	'false' AS IsDataVault,
	'false' AS IsMultiSource,
	COALESCE(WsObj.TargetName,'TGT_PH') AS LocationName,
	'Table' AS Materialization,
	WsObj.ObjectDescription AS ObjectType,
	RANK() OVER (PARTITION BY dt_table_name ORDER BY WsTem.TemplateName DESC) AS Rnk
FROM #SourceDbSchema.ws_dim_tab WsTab
LEFT JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsTab.dt_obj_key = WsObj.ObjectKey
LEFT JOIN #WsToClRepoSchema.vObjectTemplates WsTem
  ON WsTab.dt_obj_key = WsTem.ObjectKey
LEFT JOIN #SourceDbSchema.ws_scr_header WsUpd
  ON WsTab.dt_update_key = WsUpd.sh_obj_key
LEFT JOIN #SourceDbSchema.ws_scr_header WsCst
  ON WsTab.dt_build_key = WsCst.sh_obj_key
LEFT JOIN #SourceDbSchema.ws_source_mapping_tab WsSmt
  ON WsTab.dt_obj_key = WsSmt.smt_parent_obj_key
WHERE WsTab.dt_table_name NOT IN ('dss_fact_table','dss_source_system','dim_date')
  AND WsSmt.smt_parent_obj_key IS NULL
UNION ALL
-- FACT (NORMAL)
SELECT 
	ft_table_name AS TblName,
	COALESCE(WsTem.InsertStrategy,'Merge') AS InsertStrategy,
	COALESCE(WsTem.TruncateBefore,'false') AS TruncateBefore,
	COALESCE(WsObj.TargetDb,'SOURCE_DB') AS TargetDb,
	WsTab.ft_description AS TblDescription,
	'false' AS IsDataVault,
	'false' AS IsMultiSource,
	COALESCE(WsObj.TargetName,'TGT_PH') AS LocationName,
	'Table' AS Materialization,
	WsObj.ObjectDescription AS ObjectType,
	RANK() OVER (PARTITION BY ft_table_name ORDER BY WsTem.TemplateName DESC) AS Rnk
FROM #SourceDbSchema.ws_fact_tab WsTab
LEFT JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsTab.ft_obj_key = WsObj.ObjectKey
LEFT JOIN #WsToClRepoSchema.vObjectTemplates WsTem
  ON WsTab.ft_obj_key = WsTem.ObjectKey
LEFT JOIN #SourceDbSchema.ws_scr_header WsUpd
  ON WsTab.ft_update_key = WsUpd.sh_obj_key
LEFT JOIN #SourceDbSchema.ws_scr_header WsCst
  ON WsTab.ft_build_key = WsCst.sh_obj_key
LEFT JOIN #SourceDbSchema.ws_source_mapping_tab WsSmt
  ON WsTab.ft_obj_key = WsSmt.smt_parent_obj_key
WHERE WsSmt.smt_parent_obj_key IS NULL  
UNION ALL
-- LOAD
SELECT 
	lt_table_name AS TblName,
	'Insert' AS InsertStrategy,
	'true' AS TruncateBefore,
	COALESCE(WsObj.TargetDb,'SOURCE_DB') AS TargetDb,
	WsTab.lt_description AS TblDescription,
	'false'	AS IsDataVault,
	'false' AS IsMultiSource,
	COALESCE(WsObj.TargetName,'TGT_PH') AS LocationName,
	'Table' AS Materialization,
	WsObj.ObjectDescription AS ObjectType,
	1 AS Rnk
FROM #SourceDbSchema.ws_load_tab WsTab
LEFT JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsTab.lt_obj_key = WsObj.ObjectKey
UNION ALL
-- EDW 3NF (NORMAL)
SELECT 
	nt_table_name AS TblName,
	COALESCE(WsTem.InsertStrategy,'Merge') AS InsertStrategy,
	COALESCE(WsTem.TruncateBefore,'false') AS TruncateBefore,
	COALESCE(WsObj.TargetDb,'SOURCE_DB') AS TargetDb,
	WsTab.nt_description AS TblDescription,
	CASE
		WHEN WsObj.ObjectDescription IN ('Hub','Link','Satellite') 
		THEN 'true'
		ELSE 'false'
	END	AS IsDataVault,
	'false' AS IsMultiSource,
	COALESCE(WsObj.TargetName,'TGT_PH') AS LocationName,
	'Table' AS Materialization,
	WsObj.ObjectDescription AS ObjectType,
	RANK() OVER (PARTITION BY nt_table_name ORDER BY WsTem.TemplateName DESC) AS Rnk
FROM #SourceDbSchema.ws_normal_tab WsTab
LEFT JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsTab.nt_obj_key = WsObj.ObjectKey
LEFT JOIN #WsToClRepoSchema.vObjectTemplates WsTem
  ON WsTab.nt_obj_key = WsTem.ObjectKey
LEFT JOIN #SourceDbSchema.ws_scr_header WsUpd
  ON WsTab.nt_update_key = WsUpd.sh_obj_key
LEFT JOIN #SourceDbSchema.ws_scr_header WsCst
  ON WsTab.nt_build_key = WsCst.sh_obj_key
LEFT JOIN #SourceDbSchema.ws_source_mapping_tab WsSmt
  ON WsTab.nt_obj_key = WsSmt.smt_parent_obj_key
WHERE WsSmt.smt_parent_obj_key IS NULL  
UNION ALL
-- DATA STORE (NORMAL)
SELECT 
	ot_table_name AS TblName,
	COALESCE(WsTem.InsertStrategy,'Merge') AS InsertStrategy,
	COALESCE(WsTem.TruncateBefore,'false') AS TruncateBefore,
	COALESCE(WsObj.TargetDb,'SOURCE_DB') AS TargetDb,
	WsTab.ot_description AS TblDescription,
	'false' AS IsDataVault,
	'false' AS IsMultiSource,
	COALESCE(WsObj.TargetName,'TGT_PH') AS LocationName,
	'Table' AS Materialization,
	WsObj.ObjectDescription AS ObjectType,
	RANK() OVER (PARTITION BY ot_table_name ORDER BY WsTem.TemplateName DESC) AS Rnk
FROM #SourceDbSchema.ws_ods_tab WsTab
LEFT JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsTab.ot_obj_key = WsObj.ObjectKey
LEFT JOIN #WsToClRepoSchema.vObjectTemplates WsTem
  ON WsTab.ot_obj_key = WsTem.ObjectKey
LEFT JOIN #SourceDbSchema.ws_scr_header WsUpd
  ON WsTab.ot_update_key = WsUpd.sh_obj_key
LEFT JOIN #SourceDbSchema.ws_scr_header WsCst
  ON WsTab.ot_build_key = WsCst.sh_obj_key
LEFT JOIN #SourceDbSchema.ws_source_mapping_tab WsSmt
  ON WsTab.ot_obj_key = WsSmt.smt_parent_obj_key
WHERE WsSmt.smt_parent_obj_key IS NULL  
UNION ALL
-- RETRO
SELECT 
	rt_table_name AS TblName,
	'NA' AS TemplateName,
	'false' AS TruncateBefore,
	COALESCE(WsObj.TargetDb,'SOURCE_DB') AS TargetDb,
	WsTab.rt_description AS TblDescription,
	'false'	AS IsDataVault,
	'false' AS IsMultiSource,
	COALESCE(WsObj.TargetName,'TGT_PH') AS LocationName,
	'false' AS Materialization,
	'Source' AS ObjectType,
	1 AS Rnk
FROM #SourceDbSchema.ws_retro_tab WsTab
LEFT JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsTab.rt_obj_key = WsObj.ObjectKey
UNION ALL
-- STAGE (NORMAL)
SELECT 
	st_table_name AS TblName,
	COALESCE(WsTem.InsertStrategy,'Insert') AS InsertStrategy,
	COALESCE(WsTem.TruncateBefore,'true') AS TruncateBefore,
	COALESCE(WsObj.TargetDb,'SOURCE_DB') AS TargetDb,
	WsTab.st_description AS TblDescription,
	'false'	AS IsDataVault,
	'false' AS IsMultiSource,
	COALESCE(WsObj.TargetName,'TGT_PH') AS LocationName,
	'Table' AS Materialization,
	WsObj.ObjectDescription AS ObjectType,
	RANK() OVER (PARTITION BY st_table_name ORDER BY WsTem.TemplateName DESC) AS Rnk
FROM #SourceDbSchema.ws_stage_tab WsTab
LEFT JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsTab.st_obj_key = WsObj.ObjectKey
LEFT JOIN #WsToClRepoSchema.vObjectTemplates WsTem
  ON WsTab.st_obj_key = WsTem.ObjectKey
LEFT JOIN #SourceDbSchema.ws_scr_header WsUpd
  ON WsTab.st_update_key = WsUpd.sh_obj_key
LEFT JOIN #SourceDbSchema.ws_scr_header WsCst
  ON WsTab.st_build_key = WsCst.sh_obj_key
LEFT JOIN #SourceDbSchema.ws_source_mapping_tab WsSmt
  ON WsTab.st_obj_key = WsSmt.smt_parent_obj_key
WHERE WsSmt.smt_parent_obj_key IS NULL
UNION ALL
-- VIEW
SELECT 
	vt_table_name AS TblName,
	'NA' AS TemplateName,
	'false' AS TruncateBefore,
	COALESCE(WsObj.TargetDb,'SOURCE_DB') AS TargetDb,
	WsTab.vt_description AS TblDescription,
	'false'	AS IsDataVault,
	'false' AS IsMultiSource,
	COALESCE(WsObj.TargetName,'TGT_PH') AS LocationName,
	'View' AS Materialization,
	WsObj.ObjectDescription AS ObjectType,
	1 AS Rnk
FROM #SourceDbSchema.ws_view_tab WsTab
LEFT JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsTab.vt_obj_key = WsObj.ObjectKey
;