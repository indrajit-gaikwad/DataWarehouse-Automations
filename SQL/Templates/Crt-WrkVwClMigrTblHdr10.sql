CREATE OR ALTER VIEW #WsToClRepoSchema.WrkVwClMigrTblHdr10 (TblName, InsertStrategy, TruncateBefore, TargetDb, TblDescription, IsDataVault, IsMultiSource, LocationName, Materialization, ObjectType, Rnk) AS 
-- AGGREGATE (SOURCE MAPPING)
SELECT 
	at_table_name AS TblName,
	COALESCE(WsTmr.InsertStrategy,'Merge') AS InsertStrategy,
	COALESCE(WsTmr.TruncateBefore,'False') AS TruncateBefore,
	COALESCE(WsObj.TargetDb,'PH_DATABASE') AS TargetDb,
	WsTab.at_description AS TblDescription,
	'False' AS IsDataVault,
	'True' AS IsMultiSource,
	COALESCE(WsObj.TargetName,'TGT_PH') AS LocationName,
	'Table' AS Materialization,
	WsObj.ObjectDescription AS ObjectType,
	RANK() OVER (PARTITION BY at_table_name ORDER BY WsTmr.TemplateName DESC) AS Rnk
FROM #SourceDbSchema.ws_agg_tab WsTab
JOIN #SourceDbSchema.ws_source_mapping_tab WsSmt
  ON WsTab.at_obj_key = WsSmt.smt_parent_obj_key
JOIN #SourceDbSchema.ws_source_mapping_att WsSma
  ON WsSmt.smt_source_mapping_key = WsSma.sma_source_mapping_key
JOIN #SourceDbSchema.ws_tem_header WsTmh 
  ON WsSma.sma_update_template_key = WsTmh.th_obj_key  
LEFT JOIN #WsToClRepoSchema.RefTemplateMaster WsTmr
  ON WsTmh.th_name = WsTmr.TemplateName
LEFT JOIN #SourceDbSchema.ws_scr_header WsUpd
  ON WsSmt.smt_update_key = WsUpd.sh_obj_key
LEFT JOIN #SourceDbSchema.ws_scr_header WsCst
  ON WsSmt.smt_build_key = WsCst.sh_obj_key
LEFT JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsTab.at_obj_key = WsObj.ObjectKey
UNION ALL  
-- DIMENSION (SOURCE MAPPING)
SELECT 
	dt_table_name AS TblName,
	COALESCE(WsTmr.InsertStrategy,'Merge') AS InsertStrategy,
	COALESCE(WsTmr.TruncateBefore,'False') AS TruncateBefore,
	COALESCE(WsObj.TargetDb,'SOURCE_DB') AS TargetDb,
	WsTab.dt_description AS TblDescription,
	'False' AS IsDataVault,
	'True' AS IsMultiSource,
	COALESCE(WsObj.TargetName,'TGT_PH') AS LocationName,
	'Table' AS Materialization,
	WsObj.ObjectDescription AS ObjectType,
	RANK() OVER (PARTITION BY dt_table_name ORDER BY WsTmr.TemplateName DESC) AS Rnk
FROM #SourceDbSchema.ws_dim_tab WsTab
JOIN #SourceDbSchema.ws_source_mapping_tab WsSmt
  ON WsTab.dt_obj_key = WsSmt.smt_parent_obj_key
JOIN #SourceDbSchema.ws_source_mapping_att WsSma
  ON WsSmt.smt_source_mapping_key = WsSma.sma_source_mapping_key
JOIN #SourceDbSchema.ws_tem_header WsTmh 
  ON WsSma.sma_update_template_key = WsTmh.th_obj_key  
LEFT JOIN #WsToClRepoSchema.RefTemplateMaster WsTmr
  ON WsTmh.th_name = WsTmr.TemplateName
LEFT JOIN #SourceDbSchema.ws_scr_header WsUpd
  ON WsSmt.smt_update_key = WsUpd.sh_obj_key
LEFT JOIN #SourceDbSchema.ws_scr_header WsCst
  ON WsSmt.smt_build_key = WsCst.sh_obj_key  
LEFT JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsTab.dt_obj_key = WsObj.ObjectKey
WHERE WsTab.dt_table_name NOT IN ('dss_fact_table','dss_source_system','dim_date') 
UNION ALL
-- FACT (SOURCE MAPPING)
SELECT 
	ft_table_name AS TblName,
	COALESCE(WsTmr.InsertStrategy,'Merge') AS InsertStrategy,
	COALESCE(WsTmr.TruncateBefore,'False') AS TruncateBefore,
	COALESCE(WsObj.TargetDb,'SOURCE_DB') AS TargetDb,
	WsTab.ft_description AS TblDescription,
	'False' AS IsDataVault,
	'True' AS IsMultiSource,
	COALESCE(WsObj.TargetName,'TGT_PH') AS LocationName,
	'Table' AS Materialization,
	WsObj.ObjectDescription AS ObjectType,
	RANK() OVER (PARTITION BY ft_table_name ORDER BY WsTmr.TemplateName DESC) AS Rnk
FROM #SourceDbSchema.ws_fact_tab WsTab
JOIN #SourceDbSchema.ws_source_mapping_tab WsSmt
  ON WsTab.ft_obj_key = WsSmt.smt_parent_obj_key
JOIN #SourceDbSchema.ws_source_mapping_att WsSma
  ON WsSmt.smt_source_mapping_key = WsSma.sma_source_mapping_key
JOIN #SourceDbSchema.ws_tem_header WsTmh 
  ON WsSma.sma_update_template_key = WsTmh.th_obj_key  
LEFT JOIN #WsToClRepoSchema.RefTemplateMaster WsTmr
  ON WsTmh.th_name = WsTmr.TemplateName
LEFT JOIN #SourceDbSchema.ws_scr_header WsUpd
  ON WsSmt.smt_update_key = WsUpd.sh_obj_key
LEFT JOIN #SourceDbSchema.ws_scr_header WsCst
  ON WsSmt.smt_build_key = WsCst.sh_obj_key
LEFT JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsTab.ft_obj_key = WsObj.ObjectKey
UNION ALL
-- EDW 3NF (SOURCE MAPPING)
SELECT 
	nt_table_name AS TblName,
	COALESCE(WsTmr.InsertStrategy,'Merge') AS InsertStrategy,
	COALESCE(WsTmr.TruncateBefore,'False') AS TruncateBefore,
	COALESCE(WsObj.TargetDb,'SOURCE_DB') AS TargetDb,
	WsTab.nt_description AS TblDescription,
	CASE
		WHEN WsObj.ObjectDescription IN ('Hub','Link','Satellite') 
		THEN 'True'
		ELSE 'False'
	END	AS IsDataVault,
	'True' AS IsMultiSource,
	COALESCE(WsObj.TargetName,'TGT_PH') AS LocationName,
	'Table' AS Materialization,
	WsObj.ObjectDescription AS ObjectType,
	RANK() OVER (PARTITION BY nt_table_name ORDER BY WsTmr.TemplateName DESC) AS Rnk
FROM #SourceDbSchema.ws_normal_tab WsTab
JOIN #SourceDbSchema.ws_source_mapping_tab WsSmt
  ON WsTab.nt_obj_key = WsSmt.smt_parent_obj_key
JOIN #SourceDbSchema.ws_source_mapping_att WsSma
  ON WsSmt.smt_source_mapping_key = WsSma.sma_source_mapping_key
JOIN #SourceDbSchema.ws_tem_header WsTmh 
  ON WsSma.sma_update_template_key = WsTmh.th_obj_key  
LEFT JOIN #WsToClRepoSchema.RefTemplateMaster WsTmr
  ON WsTmh.th_name = WsTmr.TemplateName
LEFT JOIN #SourceDbSchema.ws_scr_header WsUpd
  ON WsSmt.smt_update_key = WsUpd.sh_obj_key
LEFT JOIN #SourceDbSchema.ws_scr_header WsCst
  ON WsSmt.smt_build_key = WsCst.sh_obj_key  
LEFT JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsTab.nt_obj_key = WsObj.ObjectKey
UNION ALL
-- DATA STORE (SOURCE MAPPING)
SELECT 
	ot_table_name AS TblName,
	COALESCE(WsTmr.InsertStrategy,'Merge') AS InsertStrategy,
	COALESCE(WsTmr.TruncateBefore,'False') AS TruncateBefore,
	COALESCE(WsObj.TargetDb,'SOURCE_DB') AS TargetDb,
	WsTab.ot_description AS TblDescription,
	'False' AS IsDataVault,
	'True' AS IsMultiSource,
	COALESCE(WsObj.TargetName,'TGT_PH') AS LocationName,
	'Table' AS Materialization,
	WsObj.ObjectDescription AS ObjectType,
	RANK() OVER (PARTITION BY ot_table_name ORDER BY WsTmr.TemplateName DESC) AS Rnk
FROM #SourceDbSchema.ws_ods_tab WsTab
JOIN #SourceDbSchema.ws_source_mapping_tab WsSmt
  ON WsTab.ot_obj_key = WsSmt.smt_parent_obj_key
JOIN #SourceDbSchema.ws_source_mapping_att WsSma
  ON WsSmt.smt_source_mapping_key = WsSma.sma_source_mapping_key
JOIN #SourceDbSchema.ws_tem_header WsTmh 
  ON WsSma.sma_update_template_key = WsTmh.th_obj_key  
LEFT JOIN #WsToClRepoSchema.RefTemplateMaster WsTmr
  ON WsTmh.th_name = WsTmr.TemplateName
LEFT JOIN #SourceDbSchema.ws_scr_header WsUpd
  ON WsSmt.smt_update_key = WsUpd.sh_obj_key
LEFT JOIN #SourceDbSchema.ws_scr_header WsCst
  ON WsSmt.smt_build_key = WsCst.sh_obj_key
LEFT JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsTab.ot_obj_key = WsObj.ObjectKey
UNION ALL
-- STAGE (SOURCE MAPPING)
SELECT 
	st_table_name AS TblName,
	COALESCE(WsTmr.InsertStrategy,'Merge') AS InsertStrategy,
	COALESCE(WsTmr.TruncateBefore,'False') AS TruncateBefore,
	COALESCE(WsObj.TargetDb,'SOURCE_DB') AS TargetDb,
	WsTab.st_description AS TblDescription,
	'False'	AS IsDataVault,
	'True' AS IsMultiSource,
	COALESCE(WsObj.TargetName,'TGT_PH') AS LocationName,
	'Table' AS Materialization,
	WsObj.ObjectDescription AS ObjectType,
	RANK() OVER (PARTITION BY st_table_name ORDER BY WsTmr.TemplateName DESC) AS Rnk
FROM #SourceDbSchema.ws_stage_tab WsTab
JOIN #SourceDbSchema.ws_source_mapping_tab WsSmt
  ON WsTab.st_obj_key = WsSmt.smt_parent_obj_key
JOIN #SourceDbSchema.ws_source_mapping_att WsSma
  ON WsSmt.smt_source_mapping_key = WsSma.sma_source_mapping_key
JOIN #SourceDbSchema.ws_tem_header WsTmh 
  ON WsSma.sma_update_template_key = WsTmh.th_obj_key  
LEFT JOIN #WsToClRepoSchema.RefTemplateMaster WsTmr
  ON WsTmh.th_name = WsTmr.TemplateName
LEFT JOIN #SourceDbSchema.ws_scr_header WsUpd
  ON WsSmt.smt_update_key = WsUpd.sh_obj_key
LEFT JOIN #SourceDbSchema.ws_scr_header WsCst
  ON WsSmt.smt_build_key = WsCst.sh_obj_key  
LEFT JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsTab.st_obj_key = WsObj.ObjectKey
;