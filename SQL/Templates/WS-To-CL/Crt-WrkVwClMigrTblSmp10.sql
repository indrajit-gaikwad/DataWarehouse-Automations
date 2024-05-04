CREATE OR ALTER VIEW #WsToClRepoSchema.WrkVwClMigrTblSmp10
(
	TblName,
	SrcTblName,
	SrcTblLocation,
	TblType,
	JoinCond,
	SmpName
) AS
SELECT DISTINCT
	WsTab.oo_name,
	WsSmc.smc_src_table,
	COALESCE(WsObj.TargetName,'TGT_PH'),
	WsObj.ObjectDescription,
	REPLACE(REPLACE(REPLACE(CONVERT(varchar(max),WsSmp.smt_where),'[TABLEOWNER].[','{{ ref('''+ COALESCE(WsObj.TargetName,'TGT_PH') + ''','''),']',''') }}'),'~',' '),	
	WsSmp.smt_table_name 
FROM #SourceDbSchema.ws_obj_object WsTab
JOIN #SourceDbSchema.ws_source_mapping_tab WsSmp
  ON WsTab.oo_obj_key = WsSmp.smt_parent_obj_key
JOIN #SourceDbSchema.ws_source_mapping_col WsSmc
  ON WsSmp.smt_source_mapping_key = WsSmc.smc_source_mapping_key 
JOIN #WsToClRepoSchema.ObjectDetails WsObj
  ON WsSmc.smc_src_table = WsObj.ObjectName  
WHERE WsSmp.smt_parent_obj_key IS NOT NULL 
  AND LEN(WsSmc.smc_src_table) > 1
;