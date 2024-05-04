------------------------------------------------------------------------------------------ 
-- Script	: Crt-vObjectTemplates
-- Author	: Indrajit Gaikwad
-- Date  	: 15-AUG-2023
-- Purpose	: Create Work View - vObjectTemplates
------------------------------------------------------------------------------------------- 

CREATE OR ALTER VIEW #WsToClRepoSchema.vObjectTemplates (ClientCd, ObjectKey, TemplateName, InsertStrategy, TruncateBefore) AS 
SELECT 
	'#ClientCode',
	ta_obj_key,
	th_name,
	InsertStrategy,
	TruncateBefore
FROM #SourceDbSchema.ws_table_attributes
JOIN #SourceDbSchema.ws_tem_header
  ON ta_val_1 = th_obj_key
 AND ta_type  = 'L'
JOIN #WsToClRepoSchema.RefTemplateMaster
  ON th_name = TemplateName
UNION ALL
SELECT 
	'#ClientCode',
	ta_obj_key,
	th_name,
	InsertStrategy,
	TruncateBefore
FROM #SourceDbSchema.ws_table_attributes
JOIN #SourceDbSchema.ws_tem_header
  ON ta_val_2 = th_obj_key
 AND ta_type  = 'L'
JOIN #WsToClRepoSchema.RefTemplateMaster
  ON th_name = TemplateName
UNION ALL
SELECT 
	'#ClientCode',
	ta_obj_key,
	th_name,
	InsertStrategy,
	TruncateBefore
FROM #SourceDbSchema.ws_table_attributes
JOIN #SourceDbSchema.ws_tem_header
  ON ta_val_3 = th_obj_key
 AND ta_type  = 'L'
JOIN #WsToClRepoSchema.RefTemplateMaster
  ON th_name = TemplateName
UNION ALL
SELECT 
	'#ClientCode',
	ta_obj_key,
	th_name,
	InsertStrategy,
	TruncateBefore
FROM #SourceDbSchema.ws_table_attributes
JOIN #SourceDbSchema.ws_tem_header
  ON ta_val_4 = th_obj_key
 AND ta_type  = 'L'
JOIN #WsToClRepoSchema.RefTemplateMaster
  ON th_name = TemplateName
UNION ALL
SELECT 
	'#ClientCode',
	ta_obj_key,
	th_name,
	InsertStrategy,
	TruncateBefore
FROM #SourceDbSchema.ws_table_attributes
JOIN #SourceDbSchema.ws_tem_header
  ON ta_val_5 = th_obj_key
 AND ta_type  = 'L'
JOIN #WsToClRepoSchema.RefTemplateMaster
  ON th_name = TemplateName
UNION ALL
SELECT 
	'#ClientCode',
	ta_obj_key,
	th_name,
	InsertStrategy,
	TruncateBefore
FROM #SourceDbSchema.ws_table_attributes
JOIN #SourceDbSchema.ws_tem_header
  ON ta_val_6 = th_obj_key
 AND ta_type  = 'L'
JOIN #WsToClRepoSchema.RefTemplateMaster
  ON th_name = TemplateName
UNION ALL
SELECT 
	'#ClientCode',
	ta_obj_key,
	th_name,
	InsertStrategy,
	TruncateBefore
FROM #SourceDbSchema.ws_table_attributes
JOIN #SourceDbSchema.ws_tem_header
  ON ta_val_7 = th_obj_key
 AND ta_type  = 'L'
JOIN #WsToClRepoSchema.RefTemplateMaster
  ON th_name = TemplateName
UNION ALL
SELECT 
	'#ClientCode',
	ta_obj_key,
	th_name,
	InsertStrategy,
	TruncateBefore
FROM #SourceDbSchema.ws_table_attributes
JOIN #SourceDbSchema.ws_tem_header
  ON ta_val_8 = th_obj_key
 AND ta_type  = 'L'
JOIN #WsToClRepoSchema.RefTemplateMaster
  ON th_name = TemplateName
UNION ALL
SELECT 
	'#ClientCode',
	ta_obj_key,
	th_name,
	InsertStrategy,
	TruncateBefore
FROM #SourceDbSchema.ws_table_attributes
JOIN #SourceDbSchema.ws_tem_header
  ON ta_val_9 = th_obj_key
 AND ta_type  = 'L'
JOIN #WsToClRepoSchema.RefTemplateMaster
  ON th_name = TemplateName
UNION ALL
SELECT 
	'#ClientCode',
	ta_obj_key,
	th_name,
	InsertStrategy,
	TruncateBefore
FROM #SourceDbSchema.ws_table_attributes
JOIN #SourceDbSchema.ws_tem_header
  ON ta_val_10 = th_obj_key
 AND ta_type  = 'L'
JOIN #WsToClRepoSchema.RefTemplateMaster
  ON th_name = TemplateName
UNION ALL
SELECT 
	'#ClientCode',
	ta_obj_key,
	th_name,
	InsertStrategy,
	TruncateBefore
FROM #SourceDbSchema.ws_table_attributes
JOIN #SourceDbSchema.ws_tem_header
  ON ta_val_11 = th_obj_key
 AND ta_type  = 'L'
JOIN #WsToClRepoSchema.RefTemplateMaster
  ON th_name = TemplateName
UNION ALL
SELECT 
	'#ClientCode',
	ta_obj_key,
	th_name,
	InsertStrategy,
	TruncateBefore
FROM #SourceDbSchema.ws_table_attributes
JOIN #SourceDbSchema.ws_tem_header
  ON ta_val_12 = th_obj_key
 AND ta_type  = 'L'
JOIN #WsToClRepoSchema.RefTemplateMaster
  ON th_name = TemplateName
;