#------------------------------------------------------------------------------------------ 
#-- CONVERT WHERESCAPE REPOSITORY TO COALESCE REPOSITORY
#------------------------------------------------------------------------------------------
Function Convert-WS-To-CL
{
	param
	(
        [Parameter(Mandatory=$true)]
        [Hashtable]$UserParametersHashTable,
		[Parameter(Mandatory=$true)]
        [Hashtable]$FormParametersHashTable
    )   
    try
	{
			
		#------------------------------------------------------------------------------------------ 
		#-- READ PARAMETERS & ASSIGN VARIABLES
		#------------------------------------------------------------------------------------------

		$SqlUserDirectory      = "$env:USERPROFILE\DE-Automations\SQL"
		$SqlTemplatesDirectory = "$env:USERPROFILE\DE-Automations\SQL\Templates"
		
		#-- User Parameters File
		$AutomationsDirectory   = $UserParametersHashTable["AutomationsDirectory"]
		$AutomationsEmail       = $UserParametersHashTable["AutomationsEmail"] 
        $LocalSqlServer         = $UserParametersHashTable["LocalSqlServer"]
		$LocalSqlServerUser     = $UserParametersHashTable["LocalSqlServerUser"]
		$LocalSqlServerPassword = $UserParametersHashTable["LocalSqlServerPassword"]
		$CloudSqlServer         = $UserParametersHashTable["CloudSqlServer"]
		$CloudSqlServerUser     = $UserParametersHashTable["CloudSqlServerUser"]
		$CloudSqlServerPassword = $UserParametersHashTable["CloudSqlServerPassword"]		
		$WhereScapeDB      	    = $UserParametersHashTable["WhereScapeDB"]
		$WsToClRepoSchema       = $UserParametersHashTable["WsToClRepoSchema"]
		$CoalesceDB      	    = $UserParametersHashTable["CoalesceDB"]
		
		#-- Form Parameters File
		$LogID     			   = $FormParametersHashTable["LogID"]
        $ClientCode			   = $FormParametersHashTable["ClientCode"]
		$ServerType            = $FormParametersHashTable["ServerType"]
        $ServerType            = $FormParametersHashTable["ServerType"]
        $Source		           = $FormParametersHashTable["Source"]
        $SourceDB              = $FormParametersHashTable["SourceDB"]
        $ClientRepoDB          = $FormParametersHashTable["ClientRepoDB"]
		$SourceObject		   = $FormParametersHashTable["SourceObject"]
        $Target                = $FormParametersHashTable["Target"]
        $TargetDB              = $FormParametersHashTable["TargetDB"]

		#-- Derived Variables
		$SqlWorkingDirectory   = "${AutomationsDirectory}\${ClientCode}\${LogID}"	
		$WsToClRepoDbSchema    = "${WhereScapeDB}.${WsToClRepoSchema}"
		$SourceDbSchema        = "${ClientRepoDB}.dbo"
		If ($DbServerType = "Local")
		{
			$DbServer   = $LocalSqlServer
			$DbUser     = $LocalSqlServerUser
			$DbPassword = $LocalSqlServerPassword
		}
		Else
		{
			$DbServer   = $CloudSqlServer
			$DbUser     = $CloudSqlServerUser
			$DbPassword = $CloudSqlServerPassword			
		}		
				
        #-- Delete All Previously Created Temporary Files
		Remove-Item -Path $SqlWorkingDirectory\*.* -Force
	
		#------------------------------------------------------------------------------------------ 
		#-- GENERATE SQL FILES
		#------------------------------------------------------------------------------------------
		#-- Reference Tables
		(((Get-Content -Path "${SqlTemplatesDirectory}\Crt-ObjectDetails.sql") -Replace "#WsToClRepoSchema",$WsToClRepoSchema) -Replace "#SourceDbSchema",$SourceDbSchema) -Replace "#ClientCode",$ClientCode | Set-Content "${SqlWorkingDirectory}\Crt-ObjectDetails.sql"
		(((Get-Content -Path "${SqlTemplatesDirectory}\Crt-ColumnDetails.sql") -Replace "#WsToClRepoSchema",$WsToClRepoSchema) -Replace "#SourceDbSchema",$SourceDbSchema) -Replace "#ClientCode",$ClientCode | Set-Content "${SqlWorkingDirectory}\Crt-ColumnDetails.sql"
		(((Get-Content -Path "${SqlTemplatesDirectory}\Crt-vObjectTemplates.sql") -Replace "#WsToClRepoSchema",$WsToClRepoSchema) -Replace "#SourceDbSchema",$SourceDbSchema) -Replace "#ClientCode",$ClientCode | Set-Content "${SqlWorkingDirectory}\Crt-vObjectTemplates.sql"
        (((Get-Content -Path "${SqlTemplatesDirectory}\Crt-vRefAuditColumns.sql") -Replace "#WsToClRepoSchema",$WsToClRepoSchema) -Replace "#SourceDbSchema",$SourceDbSchema) -Replace "#ClientCode",$ClientCode | Set-Content "${SqlWorkingDirectory}\Crt-vRefAuditColumns.sql"
		
		#-- ETL Work Views
		If ($SourceObject -eq "Load")
		{
			(((Get-Content -Path "${SqlTemplatesDirectory}\Crt-WrkVwClMigrTblHdr05-Load.sql") -Replace "#WsToClRepoSchema",$WsToClRepoSchema) -Replace "#SourceDbSchema",$SourceDbSchema) -Replace "#ClientCode",$ClientCode | Set-Content "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblHdr05.sql"
			(((Get-Content -Path "${SqlTemplatesDirectory}\Crt-WrkVwClMigrTblDtl05-Load.sql") -Replace "#WsToClRepoSchema",$WsToClRepoSchema) -Replace "#SourceDbSchema",$SourceDbSchema) -Replace "#ClientCode",$ClientCode | Set-Content "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblDtl05.sql"
			(((Get-Content -Path "${SqlTemplatesDirectory}\Crt-WrkVwClMigrTblSmp05-Load.sql") -Replace "#WsToClRepoSchema",$WsToClRepoSchema) -Replace "#SourceDbSchema",$SourceDbSchema) -Replace "#ClientCode",$ClientCode | Set-Content "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblSmp05.sql"
		}
		Else	
		{
			(((Get-Content -Path "${SqlTemplatesDirectory}\Crt-WrkVwClMigrTblHdr05-Retro.sql") -Replace "#WsToClRepoSchema",$WsToClRepoSchema) -Replace "#SourceDbSchema",$SourceDbSchema) -Replace "#ClientCode",$ClientCode | Set-Content "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblHdr05.sql"
			(((Get-Content -Path "${SqlTemplatesDirectory}\Crt-WrkVwClMigrTblDtl05-Retro.sql") -Replace "#WsToClRepoSchema",$WsToClRepoSchema) -Replace "#SourceDbSchema",$SourceDbSchema) -Replace "#ClientCode",$ClientCode | Set-Content "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblDtl05.sql"
			(((Get-Content -Path "${SqlTemplatesDirectory}\Crt-WrkVwClMigrTblSmp05-Retro.sql") -Replace "#WsToClRepoSchema",$WsToClRepoSchema) -Replace "#SourceDbSchema",$SourceDbSchema) -Replace "#ClientCode",$ClientCode | Set-Content "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblSmp05.sql"
		}
		
		(((Get-Content -Path "${SqlTemplatesDirectory}\Crt-WrkVwClMigrTblHsh05.sql") -Replace "#WsToClRepoSchema",$WsToClRepoSchema) -Replace "#SourceDbSchema",$SourceDbSchema) -Replace "#ClientCode",$ClientCode | Set-Content "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblHsh05.sql"
		(((Get-Content -Path "${SqlTemplatesDirectory}\Crt-WrkVwClMigrTblAud05.sql") -Replace "#WsToClRepoSchema",$WsToClRepoSchema) -Replace "#SourceDbSchema",$SourceDbSchema) -Replace "#ClientCode",$ClientCode | Set-Content "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblAud05.sql"
		
		(((Get-Content -Path "${SqlTemplatesDirectory}\Crt-WrkVwClMigrTblHdr10.sql") -Replace "#WsToClRepoSchema",$WsToClRepoSchema) -Replace "#SourceDbSchema",$SourceDbSchema) -Replace "#ClientCode",$ClientCode | Set-Content "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblHdr10.sql"
		(((Get-Content -Path "${SqlTemplatesDirectory}\Crt-WrkVwClMigrTblDtl10.sql") -Replace "#WsToClRepoSchema",$WsToClRepoSchema) -Replace "#SourceDbSchema",$SourceDbSchema) -Replace "#ClientCode",$ClientCode | Set-Content "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblDtl10.sql"
		#(((Get-Content -Path "${SqlTemplatesDirectory}\Crt-WrkVwClMigrTblHsh10.sql") -Replace "#WsToClRepoSchema",$WsToClRepoSchema) -Replace "#SourceDbSchema",$SourceDbSchema) -Replace "#ClientCode",$ClientCode | Set-Content "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblHsh10.sql"
		(((Get-Content -Path "${SqlTemplatesDirectory}\Crt-WrkVwClMigrTblAud10.sql") -Replace "#WsToClRepoSchema",$WsToClRepoSchema) -Replace "#SourceDbSchema",$SourceDbSchema) -Replace "#ClientCode",$ClientCode | Set-Content "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblAud10.sql"
		(((Get-Content -Path "${SqlTemplatesDirectory}\Crt-WrkVwClMigrTblSmp10.sql") -Replace "#WsToClRepoSchema",$WsToClRepoSchema) -Replace "#SourceDbSchema",$SourceDbSchema) -Replace "#ClientCode",$ClientCode | Set-Content "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblSmp10.sql"
		
		(((Get-Content -Path "${SqlTemplatesDirectory}\Crt-WrkVwClMigrTblHdr.sql") -Replace "#WsToClRepoSchema",$WsToClRepoSchema) -Replace "#SourceDbSchema",$SourceDbSchema) -Replace "#ClientCode",$ClientCode | Set-Content "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblHdr.sql"
		(((Get-Content -Path "${SqlTemplatesDirectory}\Crt-WrkVwClMigrTblDtl.sql") -Replace "#WsToClRepoSchema",$WsToClRepoSchema) -Replace "#SourceDbSchema",$SourceDbSchema) -Replace "#ClientCode",$ClientCode | Set-Content "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblDtl.sql"
		(((Get-Content -Path "${SqlTemplatesDirectory}\Crt-WrkVwClMigrTblHsh.sql") -Replace "#WsToClRepoSchema",$WsToClRepoSchema) -Replace "#SourceDbSchema",$SourceDbSchema) -Replace "#ClientCode",$ClientCode | Set-Content "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblHsh.sql"
		(((Get-Content -Path "${SqlTemplatesDirectory}\Crt-WrkVwClMigrTblAud.sql") -Replace "#WsToClRepoSchema",$WsToClRepoSchema) -Replace "#SourceDbSchema",$SourceDbSchema) -Replace "#ClientCode",$ClientCode | Set-Content "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblAud.sql"
		(((Get-Content -Path "${SqlTemplatesDirectory}\Crt-WrkVwClMigrTblSmp.sql") -Replace "#WsToClRepoSchema",$WsToClRepoSchema) -Replace "#SourceDbSchema",$SourceDbSchema) -Replace "#ClientCode",$ClientCode | Set-Content "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblSmp.sql"
		(((Get-Content -Path "${SqlTemplatesDirectory}\Crt-WrkVwClMigrTblTrl.sql") -Replace "#WsToClRepoSchema",$WsToClRepoSchema) -Replace "#SourceDbSchema",$SourceDbSchema) -Replace "#ClientCode",$ClientCode | Set-Content "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblTrl.sql"
        
		#------------------------------------------------------------------------------------------ 
		#-- EXECUTE SQL
		#------------------------------------------------------------------------------------------
		#-- Reference Tables
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $WhereScapeDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlWorkingDirectory}\Crt-ObjectDetails.sql"
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $WhereScapeDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlWorkingDirectory}\Crt-ColumnDetails.sql"
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $WhereScapeDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlWorkingDirectory}\Crt-vObjectTemplates.sql"
        Invoke-Sqlcmd -ServerInstance $DbServer -Database $WhereScapeDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlWorkingDirectory}\Crt-vRefAuditColumns.sql"

		#-- ETL Work Views
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $WhereScapeDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblHdr05.sql"
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $WhereScapeDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblDtl05.sql"
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $WhereScapeDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblHsh05.sql"
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $WhereScapeDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblAud05.sql"
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $WhereScapeDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblSmp05.sql"

		Invoke-Sqlcmd -ServerInstance $DbServer -Database $WhereScapeDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblHdr10.sql"
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $WhereScapeDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblDtl10.sql"
		#Invoke-Sqlcmd -ServerInstance $DbServer -Database $WhereScapeDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblHsh10.sql"
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $WhereScapeDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblAud10.sql"
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $WhereScapeDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblSmp10.sql"
		
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $WhereScapeDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblHdr.sql"
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $WhereScapeDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblDtl.sql"
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $WhereScapeDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblHsh.sql"
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $WhereScapeDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblAud.sql"
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $WhereScapeDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblSmp.sql"
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $WhereScapeDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlWorkingDirectory}\Crt-WrkVwClMigrTblTrl.sql"

		#-- Process Load Tables
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $CoalesceDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlUserDirectory}\Ins-LdMigrTblHdr.sql"
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $CoalesceDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlUserDirectory}\Ins-LdMigrTblDtl.sql"
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $CoalesceDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlUserDirectory}\Ins-LdMigrTblHsh.sql"
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $CoalesceDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlUserDirectory}\Ins-LdMigrTblAud.sql"
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $CoalesceDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlUserDirectory}\Ins-LdMigrTblSmp.sql"
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $CoalesceDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlUserDirectory}\Ins-LdMigrTblTrl.sql"
		
		#-- Process Target Tables
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $CoalesceDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlUserDirectory}\Ins-DsMigrTblHdr.sql"
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $CoalesceDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlUserDirectory}\Ins-DsMigrTblDtl.sql"
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $CoalesceDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlUserDirectory}\Ins-DsMigrTblHsh.sql"
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $CoalesceDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlUserDirectory}\Ins-DsMigrTblAud.sql"
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $CoalesceDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlUserDirectory}\Ins-DsMigrTblSmp.sql"
		Invoke-Sqlcmd -ServerInstance $DbServer -Database $CoalesceDB -UserName $DbUser -Password $DbPassword -InputFile "${SqlUserDirectory}\Ins-DsMigrTblTrl.sql"
		
		$Global:ResultOfStep="Success"
		$ReturnCode = 0	
	}catch 
	{
		$Global:ResultOfStep="Failure"
		$ReturnCode = 1
    }
	Return $ReturnCode
}

#------------------------------------------------------------------------------------------ 
#-- Client-Specific-Updates: APPLY CLIENT SPECIFIC PATCH (Database Related)
#------------------------------------------------------------------------------------------
Function Client-Specific-Updates
{
	param
	(
        [Parameter(Mandatory=$true)]
        [Hashtable]$UserParametersHashTable,
		[Parameter(Mandatory=$true)]
        [Hashtable]$FormParametersHashTable
    )
	#------------------------------------------------------------------------------------------ 
	#-- READ PARAMETERS & ASSIGN VARIABLES
	#------------------------------------------------------------------------------------------

	$SqlUserDirectory      = "$env:USERPROFILE\DE-Automations\SQL"
	$SqlTemplatesDirectory = "$env:USERPROFILE\DE-Automations\SQL\Templates"
	
	#-- User Parameters File
	$AutomationsDirectory   = $UserParametersHashTable["AutomationsDirectory"]
	$AutomationsEmail       = $UserParametersHashTable["AutomationsEmail"] 
	$LocalSqlServer         = $UserParametersHashTable["LocalSqlServer"]
	$LocalSqlServerUser     = $UserParametersHashTable["LocalSqlServerUser"]
	$LocalSqlServerPassword = $UserParametersHashTable["LocalSqlServerPassword"]
	$CloudSqlServer         = $UserParametersHashTable["CloudSqlServer"]
	$CloudSqlServerUser     = $UserParametersHashTable["CloudSqlServerUser"]
	$CloudSqlServerPassword = $UserParametersHashTable["CloudSqlServerPassword"]		
	$WhereScapeDB      	    = $UserParametersHashTable["WhereScapeDB"]
	$WsToClRepoSchema       = $UserParametersHashTable["WsToClRepoSchema"]
	$CoalesceDB      	    = $UserParametersHashTable["CoalesceDB"]
	
	#-- Form Parameters File
	$LogID     			   = $FormParametersHashTable["LogID"]
	$ClientCode			   = $FormParametersHashTable["ClientCode"]
	$ServerType            = $FormParametersHashTable["ServerType"]
	$ServerType            = $FormParametersHashTable["ServerType"]

	#-- Derived Variables
	$SqlWorkingDirectory   = "${AutomationsDirectory}\${ClientCode}\${LogID}"	

	If ($DbServerType = "Local")
	{
		$DbServer   = $LocalSqlServer
		$DbUser     = $LocalSqlServerUser
		$DbPassword = $LocalSqlServerPassword
	}
	Else
	{
		$DbServer   = $CloudSqlServer
		$DbUser     = $CloudSqlServerUser
		$DbPassword = $CloudSqlServerPassword			
	}
		
	$AdhocSqlDirectory     = "${AutomationsDirectory}\AdhocSQL"
	$AdhocSqlWorkDirectory = "${AutomationsDirectory}\${ClientCode}\${LogID}\AdhocSql"
	
	#-- Check If AdhocSQL Directory Exists Else Create It
	If (!(Test-Path "${AutomationsDirectory}\${ClientCode}\${LogID}\AdhocSql" -PathType Container))
	{
		New-Item -ItemType Directory -Force -Path "${AutomationsDirectory}\${ClientCode}\${LogID}\AdhocSql"
	}
	
	#-- Move Adhoc SQL Files 
	Move-Item -Path "${AdhocSqlDirectory}\*${ClientCode}*.sql" -Destination $AdhocSqlWorkDirectory -Force

	#-- Check If The Directory Is Not Empty
	$SqlFiles = Get-ChildItem -Path $AdhocSqlWorkDirectory -Filter *.sql | Sort-Object

    If ($SqlFiles.Count -gt 0) 
	{
        # Create an array to store query results
        $QueryResults = @()
        # Loop through each SQL file and read its content
        For ($i = 0; $i -lt $SqlFiles.Count; $i++)
	  	{
            $SqlFile      = $SqlFiles[$i]  
            $QueryResult  = ""	
            $ResultString = ""
            $Result       = @()			
            
            # Read the content of the SQL file
            $SqlContent = Get-Content $SqlFile.FullName
	  	    $SqlContent += "; SELECT @@RowCount as rowsAffected"
	  	    $SqlContent=$SqlContent| Out-String
            
            # Execute the SQL query and capture the result
            try
	  		{
	  		  $Result      = Invoke-Sqlcmd -ServerInstance $DbServer -Database $CoalesceDB -UserName $DbUser -Password $DbPassword -Query $SqlContent -ErrorAction Stop
	  		  $QueryResult = 'Success'
	  		}
	  		catch
	  		{
	  		  $QueryResult='Failure'
	  		}
	  		
	  		$ResultString = $($Result.rowsAffected)	| Out-String
	  		# Create variable name
            $VariableName = "var" + ($i + 1).ToString("00")
            # Assign resultString to variable dynamically
            New-Variable -Name $VariableName -Value $ResultString -Force         
            # Store the result in a dynamically named variable
            $VariableName = "var" + ($i + 1).ToString("00")
            New-Variable -Name $VariableName -Value $ResultAsString -Force
			
			#-- Print Results
			$SqlFileName = $SqlFile.Name
			$RecordCount = $ResultString 
	  		  
			Write-Output  "$SqlFileName"
	  		Write-Output "Result       = $QueryResult"
	  		Write-Output "Record Count = $RecordCount"	
	    }
    } 
	Else 
	{
        Write-Host "No SQL files found in the directory."
    }
}