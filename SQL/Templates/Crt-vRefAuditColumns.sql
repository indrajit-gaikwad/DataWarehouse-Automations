CREATE OR ALTER VIEW #WsToClRepoSchema.vRefAuditColumns 
(
	ClientCd,
	AuditColumnName,
	AuditColumnCategory
)
AS
SELECT 
	'#ClientCode',
	mn_name, 
	mn_object
FROM #SourceDbSchema.ws_meta_names 
WHERE mn_object IN
(
	'dss_current_flag',
	'dss_end_date',
	'dss_create_time',
	'dss_update_time',
	'dss_start_date',
	'dss_version'
);