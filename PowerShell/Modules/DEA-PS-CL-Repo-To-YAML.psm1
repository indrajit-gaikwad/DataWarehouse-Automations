#---------------------------------------------------------------------------------------------------- 
#-- Convert-Source-To-YAML: Function To Generate YAML Files From Coalesce Repository (Source Objects)
#----------------------------------------------------------------------------------------------------
Function Convert-Source-To-YAML
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
	#-- Target Objects
	$DsMigrTblHdr          	= "ODS.DsMigrTblHdr"
    $DsMigrTblDtl			= "ODS.DsMigrTblDtl"
	$DsMigrTblHsh			= "ODS.DsMigrTblHsh"
    $DsMigrTblAud			= "ODS.DsMigrTblAud"
    $DsMigrTblSmp			= "ODS.DsMigrTblSmp"
	$DsMigrTblTrl			= "ODS.DsMigrTblTrl"
	
	#-- User Parameters File
	$OneDriveMainDirectory  = $UserParametersHashTable["OneDriveMainDirectory"]
    $LocalSqlServer         = $UserParametersHashTable["LocalSqlServer"]
	$LocalSqlServerUser     = $UserParametersHashTable["LocalSqlServerUser"]
	$LocalSqlServerPassword = $UserParametersHashTable["LocalSqlServerPassword"]
	$CloudSqlServer         = $UserParametersHashTable["CloudSqlServer"]
	$CloudSqlServerUser     = $UserParametersHashTable["CloudSqlServerUser"]
	$CloudSqlServerPassword = $UserParametersHashTable["CloudSqlServerPassword"]		
	$CoalesceDB      	    = $UserParametersHashTable["CoalesceDB"]
		
	#-- Form Parameters File
    $ClientCd			   = $FormParametersHashTable["ClientCode"]
	$DbServerType          = $FormParametersHashTable["ServerType"]
    
	#-- Derived Variables
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
    $OutFilesDirectory = "$OneDriveMainDirectory\PRJ${ClientCd}\OutFiles"
				
    #------------------------------------------------------------------------------------------ 
	#-- GENERATE FILES
	#------------------------------------------------------------------------------------------
    #-- STEP-00:
    try
	{
		$HdrTblQrySrc = "SELECT * FROM $DsMigrTblHdr WHERE ClientCd = `'$($ClientCd)`' AND TblType = 'Source' ;"
		$HdrTblDataSrc = Invoke-Sqlcmd -Database $CoalesceDB -ServerInstance $DbServer -Query $HdrTblQrySrc -Username $DbUser -Password $DbPassword
		$global:resultOfStep="Success"
        Write-Host "STEP-00: Success" -ForegroundColor DarkMagenta
        Write-Host ""
	}
	catch 
	{
		$global:resultOfStep="Failure"
		Write-Host "STEP-00: Failure" -ForegroundColor DarkRed
        Write-Host ""
    }   
	
	$YamlObjectSrc = @()
    
    ForEach($HdrTblRecordSrc in $HdrTblDataSrc)
    {  
        #-- STEP-01:
        try
        {	
    	    $DtlTblQrySrc   = "SELECT * FROM $DsMigrTblDtl WHERE ClientCd = `'$($ClientCd)`' AND TblId = `'$($HdrTblRecordSrc.TblId)`' Order By ColOrder;"
            $DtlTblDataSrc  = Invoke-Sqlcmd -Database $CoalesceDB -ServerInstance $DbServer -Query $DtlTblQrySrc -Username $DbUser -Password $DbPassword
            $TrlTblQrySrc   = "SELECT * FROM $DsMigrTblTrl WHERE ClientCd = `'$($ClientCd)`' AND TblId = `'$($HdrTblRecordSrc.TblId)`';"
    	    $TrlTblDataSrc  = Invoke-Sqlcmd -Database $CoalesceDB -ServerInstance $DbServer -Query $TrlTblQrySrc -Username $DbUser -Password $DbPassword
            $TrlTblQrySrcD  = "SELECT DISTINCT TblName, TblId FROM $DsMigrTblTrl WHERE ClientCd = `'$($ClientCd)`' AND TblId = `'$($HdrTblRecordSrc.TblId)`';"
            $TrlTblDataSrcD = Invoke-Sqlcmd -Database $CoalesceDB -ServerInstance $DbServer -Query $TrlTblQrySrcD -Username $DbUser -Password $DbPassword
    	    $TrlTblGrpSrc   = $TrlTblDataSrc | Group-Object -Property TblId
    	    $ColumnSrc      = @()
            $global:resultOfStep="Success"
            Write-Host "Table Name: $($TrlTblDataSrcD.TblName)" -ForegroundColor DarkBlue
            Write-Host "   STEP-01: $($TrlTblDataSrcD.TblId)(TblId)-Success" -ForegroundColor DarkCyan
	    }
        catch 
        {
	        $global:resultOfStep="Failure"
            Write-Host "   STEP-01: $($TrlTblDataSrcD.TblId)(TblId)-Failure" -ForegroundColor Red
        }
		    	
		#-- STEP-02:
        try
        {
    	    ForEach($DtlTblRecordSrc in $DtlTblDataSrc)
            {
                $ColumnSrc += [Ordered]@{
                acceptedValues = [Ordered]@{
                    strictMatch = $DtlTblRecordSrc.IsStrictMatch
                    values= @()
            }
            appliedColumnTests = [Ordered]@{}
            columnReference = [Ordered]@{
                columnCounter =  $DtlTblRecordSrc.ColId
                stepCounter =  $DtlTblRecordSrc.TblId
            }
            config = {}
            dataType = $DtlTblRecordSrc.DataType
            defaultValue= $DtlTblRecordSrc.DefaultValue
            description = """"""#$DtlTblRecordSrc.ColDescription
            name =  $DtlTblRecordSrc.ColName
            nullable = $DtlTblRecordSrc.IsNullable
            primaryKey = $DtlTblRecordSrc.IsPrimaryKey
            uniqueKey = $DtlTblRecordSrc.IsUniqueKey           
           }
        } 
                
        $YamlObjectSrc = [Ordered]@{
        "$($HdrTblRecordSrc.TblName)-$($HdrTblRecordSrc.TblId)" = [Ordered]@{
            "operation" = [Ordered]@{        
            "database" = $HdrTblRecordSrc.TblDatabase
            "dataset" = """"""
            "deployEnabled" = [System.Convert]::ToBoolean($HdrTblRecordSrc.IsDeployEnabled)
            "description" = $HdrTblRecordSrc.TblDescription
            "locationID" = """"""
            "locationName" = $HdrTblRecordSrc.LocationName
            "metadata" = [Ordered]@{            
                "columns" = $ColumnSrc            
                join= [Ordered]@{
                    joinCondition = ""
                }              
            }       
           "name" =  $TrlTblGrpSrc.Group[0].TblName
           "schema" =  $TrlTblGrpSrc.Group[0].TblSchema
           "sqlType" = "Source"
           "table" = $TrlTblGrpSrc.Group[0].TblName
           "type" = "sourceInput"
            }
         stepCounter =  $TrlTblGrpSrc.Group[0].TblId
        }
       }
        $global:resultOfStep="Success"
        #Write-Host "   STEP-02: $($DtlTblRecordSrc.ColName)-$($DtlTblRecordSrc.ColId)-Success"
	  }
      catch 
      {
	    $global:resultOfStep="Failure"
        Write-Host "   STEP-02: $($DtlTblRecordSrc.ColName)-$($DtlTblRecordSrc.ColId)-Failure"
      }

      #-- STEP-03:
      try
      {
	        $YamlContentSrc = $YamlObjectSrc| ConvertTo-Yaml
            $YamlContentSrc = $YamlContentSrc -Replace("---","").Trim() 
            $YamlContentSrc = "steps:`r`n  "+$YamlContentSrc -Replace( "\r\n", "`n")
            $global:resultOfStep="Success"
            Write-Host "   STEP-03: Convert To YAML-Success" -ForegroundColor DarkCyan
	   }
       catch 
       {
	        $global:resultOfStep="Failure"
            Write-Host "   STEP-03: Convert To YAML-Failure" -ForegroundColor Red
        }

	  #-- STEP-04:
      try
      {
	        Set-Content -Path "$($OutFilesDirectory)\$($HdrTblRecordSrc.TblType)-$($HdrTblRecordSrc.TblName)-Yaml.yml" -Value $YamlContentSrc
	        $global:resultOfStep="Success"
            Write-Host "   STEP-04: Write YAML File-Success" -ForegroundColor DarkCyan
	   }
       catch 
       {
	        $global:resultOfStep="Failure"
            Write-Host "   STEP-04: Write YAML File-Success" -ForegroundColor Red
        }
    }
	$global:resultOfStep="Success"
}

#------------------------------------------------------------------------------------------------------------ 
#-- Convert-Non-Source-To-YAML: Function To Generate YAML Files From Coalesce Repository (Non-Source Objects)
#------------------------------------------------------------------------------------------------------------
Function Convert-Non-Source-To-YAML
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
	#-- Target Objects
	$DsMigrTblHdr          	= "ODS.DsMigrTblHdr"
    $DsMigrTblDtl			= "ODS.DsMigrTblDtl"
	$DsMigrTblHsh			= "ODS.DsMigrTblHsh"
    $DsMigrTblAud			= "ODS.DsMigrTblAud"
    $DsMigrTblSmp			= "ODS.DsMigrTblSmp"
	$DsMigrTblTrl			= "ODS.DsMigrTblTrl"
	
	#-- User Parameters File
	$OneDriveMainDirectory  = $UserParametersHashTable["OneDriveMainDirectory"]
    $LocalSqlServer         = $UserParametersHashTable["LocalSqlServer"]
	$LocalSqlServerUser     = $UserParametersHashTable["LocalSqlServerUser"]
	$LocalSqlServerPassword = $UserParametersHashTable["LocalSqlServerPassword"]
	$CloudSqlServer         = $UserParametersHashTable["CloudSqlServer"]
	$CloudSqlServerUser     = $UserParametersHashTable["CloudSqlServerUser"]
	$CloudSqlServerPassword = $UserParametersHashTable["CloudSqlServerPassword"]		
	$CoalesceDB      	    = $UserParametersHashTable["CoalesceDB"]
		
	#-- Form Parameters File
    $ClientCd			   = $FormParametersHashTable["ClientCode"]
	$DbServerType          = $FormParametersHashTable["ServerType"]
    
	#-- Derived Variables
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
    $OutFilesDirectory = "$OneDriveMainDirectory\PRJ${ClientCd}\OutFiles"

    #------------------------------------------------------------------------------------------ 
	#-- GENERATE FILES
	#------------------------------------------------------------------------------------------
    #-- STEP-00:
	try
	{
		$HdrTblQryAll  = "SELECT * FROM $DsMigrTblHdr WHERE ClientCd = `'$($ClientCd)`' AND TblType <> 'Source';"
		$HdrTblDataAll = Invoke-Sqlcmd -Database $CoalesceDB -ServerInstance $DbServer -Query $HdrTblQryAll -Username $DbUser -Password $DbPassword
		$global:resultOfStep="Success"
        Write-Host "STEP-00: Success" -ForegroundColor DarkMagenta
        Write-Host ""
	}
	catch 
	{
		$global:resultOfStep="Failure"
		Write-Host "STEP-00: Failure" -ForegroundColor DarkRed
        Write-Host ""
    }   
    
	$YamlObjectAll = @()
    #-- STEP-01:
    try
    {
        ForEach($HdrTblRecordAll in $HdrTblDataAll)
        {  
	        #-- STEP-02:
            try
            {
                $DtlTblQryAll     = "SELECT * FROM $DsMigrTblDtl WHERE ClientCd = `'$($ClientCd)`' AND TblId = `'$($HdrTblRecordAll.TblId)` Order By ColOrder';"
                $DtlTblDataAll    = Invoke-Sqlcmd -Database $CoalesceDB -ServerInstance $DbServer -Query $DtlTblQryAll -Username $DbUser -Password $DbPassword
		        $HshTblQryAll     = "SELECT * FROM $DsMigrTblHsh WHERE ClientCd = `'$($ClientCd)`' AND TblId = `'$($HdrTblRecordAll.TblId)`';"
                $HshTblDataAll    = Invoke-Sqlcmd -Database $CoalesceDB -ServerInstance $DbServer -Query $HshTblQryAll -Username $DbUser -Password $DbPassword
                $AudTblQryAll     = "SELECT * FROM $DsMigrTblAud WHERE ClientCd = `'$($ClientCd)`' AND TblId = `'$($HdrTblRecordAll.TblId)`';"
                $AudTblDataAll    = Invoke-Sqlcmd -Database $CoalesceDB -ServerInstance $DbServer -Query $AudTblQryAll -Username $DbUser -Password $DbPassword
                $TrlTblQryAll     = "SELECT * FROM $DsMigrTblTrl WHERE ClientCd = `'$($ClientCd)`' AND TblId = `'$($HdrTblRecordAll.TblId)`';"
    	        $TrlTblDataAll    = Invoke-Sqlcmd -Database $CoalesceDB -ServerInstance $DbServer -Query $TrlTblQryAll -Username $DbUser -Password $DbPassword
    	        $SmpTblQryAll     = "SELECT * FROM $DsMigrTblSmp WHERE ClientCd = `'$($ClientCd)`' AND TblId = `'$($HdrTblRecordAll.TblId)`'  ORDER BY SrcTblId;"
    	        $SmpTblDataAll    = Invoke-Sqlcmd -Database $CoalesceDB -ServerInstance $DbServer -Query $SmpTblQryAll -Username $DbUser -Password $DbPassword
    	        $stringVariable   = "'" + ($SmpTblDataAll.TblId -join "', '") + "'"						
                $SmpDtlTblQryAll  = "SELECT * FROM $DsMigrTblDtl WHERE ClientCd = `'$($ClientCd)`' AND TblId = `'$($HdrTblRecordAll.TblId)`' AND TblId IN("+	$stringVariable+" ) ORDER BY SrcTblId,ColOrder;"
    	        $SmpDtlTblDataAll = Invoke-Sqlcmd -Database $CoalesceDB -ServerInstance $DbServer -Query $SmpDtlTblQryAll -Username $DbUser -Password $DbPassword
                $TrlTblQrySrcD    = "SELECT DISTINCT TblName, TblId FROM $DsMigrTblTrl WHERE ClientCd = `'$($ClientCd)`' AND TblId = `'$($HdrTblRecordAll.TblId)`';"
                $TrlTblDataSrcD   = Invoke-Sqlcmd -Database $CoalesceDB -ServerInstance $DbServer -Query $TrlTblQrySrcD -Username $DbUser -Password $DbPassword		        
                
                $global:resultOfStep="Success"
                
                Write-Host "Table Name: $($TrlTblDataSrcD.TblName)" -ForegroundColor DarkBlue
                Write-Host "   STEP-01: $DsMigrTblHdr Read-Success" -ForegroundColor DarkCyan
                Write-Host "   STEP-02: $DsMigrTblDtl Read-Success" -ForegroundColor DarkCyan
	        }
            catch 
            {
	            $global:resultOfStep="Failure"
                Write-Host "   STEP-02: $($TrlTblDataSrcD.TblId)(TblId)-Failure" -ForegroundColor Red
            }
              
	        $ColumnAll     = @()
            #-- STEP-03:
            try
            {
	            If ($SmpDtlTblDataAll -eq $null) 
                {
                    ForEach($DtlTblRecordAll in $DtlTblDataAll)
                    {
                        $DtlTblRecordAll.ColDescription
                        $SourceColumnReference = @()
                        $ColumnReference = @()
                        $ColumnReference += [Ordered]@{
                                        "columnCounter" = $($DtlTblRecordAll.SrcColId)
                                        "stepCounter" = $DtlTblRecordAll.SrcTblId
                     }
                        $SourceColumnReference += [Ordered]@{
                        "columnReferences" = $ColumnReference
    			        "transform" = Add-Double-Quotes-To-String $DtlTblRecordAll.ColTransform 
                     }
    			
                     If($DtlTblRecordAll.IsIncremental -eq "true")
                     {
                        $config = [Ordered]@{
                        "isincrementalteKey" = [System.Convert]::ToBoolean($DtlTblRecordAll.IsIncremental)
                        }
                     }
                    Else
                    {
                        $config = "{}"
                    }
				        $varBK=""
				        $varCD=""
				        If($DtlTblRecordAll.IsBusinessKey -eq "true"){
				        $varBK=$DtlTblRecordAll.IsBusinessKey
				        }
				        If($DtlTblRecordAll.IsChangeDetection -eq "true"){
				        $varCD=$DtlTblRecordAll.IsChangeDetection
				        }
                        $ColumnAll += [Ordered]@{
                        "columnReference" = [Ordered]@{
                            "columnCounter" = "$($DtlTblRecordAll.ColId)"
                            "stepCounter" = "$($DtlTblRecordAll.TblId)"
                        }
                        "config" = $config
                        "dataType" = $DtlTblRecordAll.DataType
                        "description" = "$($DtlTblRecordAll.ColDescription)"
				        "isBusinessKey"=$varBK
                        "isChangeTracking"=$varCD
                        "name"  = $DtlTblRecordAll.ColName
                        "nullable" = $DtlTblRecordAll.IsNullable
                        "sourceColumnReferences" = $SourceColumnReference
                        }
    	           }
    	  
                }
    
             Else
                {
                    $SmtTblGrpAll = $SmpDtlTblDataAll | Group-Object -Property ColName
		
                    ForEach($SmtTblRecordAll in $SmtTblGrpAll)
                    {
                        $Smt = $SmtTblRecordAll.Group[0]
                        $smtColumns = @()
    		            $SourceColumnReference = @()
                        ForEach($Value in $smtTblRecordAll.Group)
                        {
                           $ColumnReference = @()
                            $ColumnReference +=  [Ordered]@{
                            "columnCounter" = $($Value.SrcColId)
                            "stepCounter" = $Value.SrcTblId
                            }
				
                            $SourceColumnReference += [Ordered]@{
                            "columnReferences" = $ColumnReference
    			            "transform" = Add-Double-Quotes-To-String $Value.ColTransform 
                            }
                        }
    		
                        If($smt.IsIncremental -eq "true")
                        {
                            $config = [Ordered]@{
                            "isincrementalteKey" = [System.Convert]::ToBoolean($smt.IsIncremental)
                            }
                        }
                        Else
                        {
                            $config = "{}"
                        }				

				        $varBK=""
				        $varCD=""
				        If($smt.IsBusinessKey -eq "true")
                        {
				            $varBK=$smt.IsBusinessKey
				        }
				        If($smt.IsChangeDetection -eq "true")
                        {
				            $varCD=$smt.IsChangeDetection
				        }
                            $ColumnAll += [Ordered]@{
                            "columnReference" = [Ordered]@{
                            "columnCounter" = "$($smt.ColId)"
                            "stepCounter" = "$($smt.TblId)"
                            }
                            "config" = $config
                            "dataType" = $smt.DataType
                            "description" = "$($smt.ColDescription)"
				            "isBusinessKey"=$varBK
                            "isChangeTracking"=$varCD
                            "name"  = $smt.ColName
                            "nullable" = $smt.IsNullable
                            "sourceColumnReferences" = $SourceColumnReference
                            }		
			
                    }

                    $global:resultOfStep="Success"
                    Write-Host "   STEP-03: $DsMigrTblSmp Data Read-Success" -ForegroundColor DarkCyan
	            }
	   }
            catch 
            {
	          $global:resultOfStep="Failure"
              Write-Host "   STEP-03: $DsMigrTblSmp Data Read-Failure" -ForegroundColor Red
            }
            
	        #-- STEP-04:
            try
            {
                if ($HshTblDataAll -ne $null) {
                $HshTblGrpAll = $HshTblDataAll | Group-Object -Property ColId
                ForEach($HshTblRecordAll in $HshTblGrpAll)
                {
                    $Hash = $HshTblRecordAll.Group[0]
                    $hashedColumns = @()
                    ForEach($Value in $HshTblRecordAll.Group)
                    {
                        $hashedColumns += [Ordered]@{
                        columnCounter = $Value.HashColId
                        stepCounter =  $Value.HashTblId
                        }
                    }
                    $ColumnAll += [Ordered]@{
                    acceptedValues = [Ordered]@{
                    strictMatch = $Hash.IsStrictMatch
                    values= @()
                    }
                    appliedColumnTests = [Ordered]@{}
                    columnReference = [Ordered]@{
                    ` columnCounter =  $Hash.ColId
                    stepCounter =  $Hash.TblId
                    }
                    config = {}
                    dataType = $Hash.DataType
                    defaultValue=Add-Double-Quotes-To-String $Hash.DefaultValue
                    description = "$($Hash.ColDescription)"
                    hashDetails = [Ordered]@{
                    hashAlgorithm = $Hash.HashAlgorithm
                    }
                    hashedColumns = $hashedColumns
                    name =  $Hash.ColName
                    nullable = $Hash.IsNullable
                    sourceColumnReferences = [Ordered]@{
                    "columnReferences"= @()
    				"  transform" =Add-Double-Quotes-To-String $Hash.ColTransform 
                    }
                    }
                }
       }
	  $global:resultOfStep="Success"
      Write-Host "   STEP-04: $DsMigrTblHsh Data Read-Success" -ForegroundColor DarkCyan  
	  }catch {
	    $global:resultOfStep="Failure"
         Write-Host "   STEP-04: $DsMigrTblHsh Data Read-Failure" -ForegroundColor Red
      }
	  
      #-- STEP-05: 
      try
      {
        ForEach($AudTblRecordAll in $AudTblDataAll)
        {  
            $ColumnAll += [Ordered]@{
                  acceptedValues = [Ordered]@{
                      strictMatch = $AudTblRecordAll.IsStrictMatch
                      values= @()
                }
                appliedColumnTests = [Ordered]@{}
                columnReference = [Ordered]@{
                     columnCounter =  $AudTblRecordAll.ColId
                  stepCounter =  $AudTblRecordAll.TblId
                 }
                config = {}
                dataType = $AudTblRecordAll.DataType
                defaultValue=Add-Double-Quotes-To-String $AudTblRecordAll.DefaultValue
                description = "$($AudTblRecordAll.ColDescription)"
                hashedColumns = @()
                isSystemCreateDate= """"""
                name =  $AudTblRecordAll.ColName
                nullable = $AudTblRecordAll.IsNullable
                sourceColumnReferences = [Ordered]@{
                    "columnReferences"= @()
    	            "  transform" =Add-Double-Quotes-To-String $AudTblRecordAll.ColTransform
                }
            }
        }
      $global:resultOfStep="Success"
      Write-Host "   STEP-05: $DsMigrTblAud Data Read-Success" -ForegroundColor DarkCyan
	  }
      catch 
      {
	    $global:resultOfStep="Failure"
        Write-Host "   STEP-05: $DsMigrTblAud Data Read-Failure" -ForegroundColor Red
      }
      
	  
      #-- STEP-06:   		
      try
      {
        $SmpTblGrpAll = $SmpTblDataAll | Group-Object -Property TblId
        ForEach($SmpTblRecordAll in $SmpTblGrpAll)
        {

               $sourceMapping= @()	
    		   $joincondition= @()
    		   $locationName = @()
    		   
    		   for($i=0;$i -lt $SmpTblRecordAll.Group.Count ;$i++){
    		   $dependencies = @()
    		   $nolinkrefs=@()
    		   If (!$SmpTblRecordAll.Group[$i].NoLinkLocName) 
               {
                    $dependencies +=[Ordered]@{		
                    "locationName" = $SmpTblRecordAll.Group[$i].DepLocName
                    nodeName =  $SmpTblRecordAll.Group[$i].DepNodeName
                    }
    		   }
    
    		   If ($SmpTblRecordAll.Group[$i].NoLinkLocName) 
               {
                    $nolinkrefs +=[Ordered]@{		
                    "locationName" = $SmpTblRecordAll.Group[$i].NoLinkLocName
                    nodeName =  $SmpTblRecordAll.Group[$i].NoLinkNodeName
                    }
    		   }
    		   Else
               {
    			   $nolinkrefs= "[]"
    			   
    		   }
   
    		     $sourceMapping += [Ordered]@{		
    		     	   aliases = ""
                     customSQL = [Ordered]@{
                             "customSQL" = ""#"$($TrlTblGrpAll.Group[$i].CustomSql)"  
                       }
                  dependencies = $dependencies 
    		      join= [Ordered]@{
    			  joinCondition = ($($SmpTblGrpAll.Group[$i].JoinCond)) -Replace( "\r\n", "`n")  -Replace( "\n", "`n")
    		      }
    		     name = $SmpTblGrpAll.Group[$i].SmpName
    		     noLinkRefs = $nolinkrefs  
    		    }		   
    	       }
       }
	    $global:resultOfStep="Success"
        Write-Host "   STEP-06: $DsMigrTblSmp Data Read-Success" -ForegroundColor DarkCyan
	  }
      catch 
      {
	    $global:resultOfStep="Failure"
        Write-Host "   STEP-06: $DsMigrTblSmp Data Read-Failure" -ForegroundColor Red
      }
      
	   
      $TrlTblGrpAll = $TrlTblDataAll | Group-Object -Property TblId		  
	  #-- STEP-07:
      try
      {
        If ($TrlTblDataAll -ne $null) 
        {
            $YamlObjectAll = [Ordered]@{
            "$($HdrTblRecordAll.TblName)-$($HdrTblRecordAll.TblId)" = [Ordered]@{
            "operation" = [Ordered]@{
            "config" = [Ordered]@{
            "insertStrategy" = $HdrTblRecordAll.InsertStrategy
            "postSQL" = "$($HdrTblRecordAll.PostSql)" 
            "preSQL" = "$($HdrTblRecordAll.PreSql)"                      
            "testsEnabled" = [System.Convert]::ToBoolean($HdrTblRecordAll.IsTestsEnabled)
            "truncateBefore" =[System.Convert]::ToBoolean($HdrTblRecordAll.IsTruncateBefore)
             }
             "database" = $HdrTblRecordAll.TblDatabase
             "deployEnabled" = [System.Convert]::ToBoolean($HdrTblRecordAll.IsDeployEnabled)
             "description" =  $HdrTblRecordAll.TblDescription
             "isDataVault" = [System.Convert]::ToBoolean($HdrTblRecordAll.IsDataVault)
             "isMultisource" = $HdrTblRecordAll.IsMultiSource
             "locationName" = $HdrTblRecordAll.LocationName
             "materializationType" = $HdrTblRecordAll.Materialization
             "metadata" = [Ordered]@{
                 "appliedNodeTests" = "[]"
                 "columns" = $ColumnAll
                 "cteString" = """"""
                 "enabledColumnTestIDs" = "[]"
                 "sourceMapping" = $sourceMapping
                 }
            "name" = $TrlTblGrpAll.Group[0].TblName
            "overrideSQL" =  $TrlTblGrpAll.Group[0].overrideSQL
            "schema" = "$($TrlTblGrpAll.Group[0].ObjectSchema)"
            "sqlType" = $TrlTblGrpAll.Group[0].SqlType
            "type" = $TrlTblGrpAll.Group[0].ModType
            }
          stepCounter =  $TrlTblGrpAll.Group[0].TblId
         }
        }
       } 
	    $global:resultOfStep="Success"
        Write-Host "   STEP-07: $DsMigrTblTrl Data Read-Success" -ForegroundColor DarkCyan
	  }
      catch 
      {
	    $global:resultOfStep="Failure"
        Write-Host "   STEP-07: $DsMigrTblTrl Data Read-Failure" -ForegroundColor Red
      }
      
	  #-- STEP-08: 
	  try
      {
        $YamlContentAll      = $YamlObjectAll  | ConvertTo-Yaml 
        $YamlContentAll      = $YamlContentAll -Replace("---","").Trim() 
        $YamlContentAll      = "steps:`r`n  "+$YamlContentAll -Replace( "\r\n", "`n")
	    $global:resultOfStep = "Success"
        Write-Host "   STEP-08: Convert To YAML-Success" -ForegroundColor DarkCyan
	  }
      catch 
      {
	    $global:resultOfStep="Failure"
        Write-Host "   STEP-08: Convert To YAML-Failure" -ForegroundColor Red
      }
      
      #-- STEP-09: 
      try
      {
        Set-Content -Path "$($OutFilesDirectory)\$($HdrTblRecordAll.TblType)-$($HdrTblRecordAll.TblName)-Yaml.yml" -Value $YamlContentAll
        $global:resultOfStep="Success"
        Write-Host "   STEP-09: Write YAML File To Directory-Success" -ForegroundColor DarkCyan
	  }
      catch 
      {
	    $global:resultOfStep="Failure"
        Write-Host "   STEP-09: Write YAML File To Directory-Failure" -ForegroundColor Red
      }
        
    }  
	    $global:resultOfStep="Success"
	
	}
    catch 
    {
	    $global:resultOfStep="Failure"
        Write-Host "   STEP-01: $DsMigrTblHdr Data Read-Failure" -ForegroundColor Red
    }
}

#------------------------------------------------------------------------------------------ 
#-- Validate-YAML-File: Function To Validate Generated YAML File
#------------------------------------------------------------------------------------------
Function Validate-YAML-File
{
	param
    (
        [Parameter(Mandatory=$true)]
        [Hashtable]$UserParametersHashTable,
		[Parameter(Mandatory=$true)]
        [Hashtable]$FormParametersHashTable
    )
    
    Import-Module -Name Powershell-Yaml

    #------------------------------------------------------------------------------------------ 
	#-- READ PARAMETERS & ASSIGN VARIABLES
	#------------------------------------------------------------------------------------------
	#-- Validation Objects
    $DsClYamlFileHdr			= "RPT.DsClYamlFileHdr"
    $DsClYamlFileDtl	        = "RPT.DsClYamlFileDtl"
	
	
	#-- User Parameters File
	$OneDriveMainDirectory  = $UserParametersHashTable["OneDriveMainDirectory"]
    $LocalSqlServer         = $UserParametersHashTable["LocalSqlServer"]
	$LocalSqlServerUser     = $UserParametersHashTable["LocalSqlServerUser"]
	$LocalSqlServerPassword = $UserParametersHashTable["LocalSqlServerPassword"]
	$CloudSqlServer         = $UserParametersHashTable["CloudSqlServer"]
	$CloudSqlServerUser     = $UserParametersHashTable["CloudSqlServerUser"]
	$CloudSqlServerPassword = $UserParametersHashTable["CloudSqlServerPassword"]		
	$ReportingDB      	    = $UserParametersHashTable["ReportingDB"]
		
	#-- Form Parameters File
    $ClientCd			   = $FormParametersHashTable["ClientCode"]
    $DbServerType          = $FormParametersHashTable["ServerType"]
    
	#-- Derived Variables
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
    
    $OutFilesDirectory = "$OneDriveMainDirectory\PRJ${ClientCd}\OutFiles"

    If (Test-Path -Path $OutFilesDirectory -PathType Container) 
    {
        $yamlFiles = Get-ChildItem -Path $OutFilesDirectory -Filter *.yml | Sort-Object
        If ($yamlFiles.Count -gt 0) 
        {
            For ($i = 0; $i -lt $yamlFiles.Count; $i++) 
		{
            $yamlFile = $yamlFiles[$i]  
            $yamlFileRead = Get-Content -Path $yamlFile.FullName
            $combinedObjects = $yamlFileRead | ConvertFrom-Yaml
            $stepsValue = $combinedObjects.steps.Keys
            $metadata = $combinedObjects.steps."$stepsValue".operation.metadata
			$varIsMultiSource=$($combinedObjects.steps."$stepsValue".operation.isMultisource) -as [string]
			$varIsMultiSource=$varIsMultiSource.ToLower()
			If($($varIsMultiSource) -eq 'true')
			{
				$multiTable=$metadata.sourceMapping.name
			}
			Else
			{
				$multiTable=@("")
				$varIsMultiSource='false'
			}
			
			$varTblDescription="false"
			
			If($($combinedObjects.steps."$stepsValue".operation.description))
			{
				$varTblDescription="true"
			}
	        
			Foreach($srcTable in $multiTable)
			{
				$DsClYamlFileHdrInsQry = @"
				INSERT INTO $DsClYamlFileHdr
					   ( [ClientCd]
					   , [TblName]
					   , [TblDatabase]
					   , [TblDescription]
					   , [IsMultiSource]
					   , [LocationName]
					   , [SmpTblName])
				 VALUES
					   ($ClientCd 
					   ,'$($combinedObjects.steps."$stepsValue".operation.name)'
					   ,'$($combinedObjects.steps."$stepsValue".operation.database)'
					   ,'$($varTblDescription)'
					   ,'$($varIsMultiSource)'
					   ,'$($combinedObjects.steps."$stepsValue".operation.locationName)'
					   ,'$($srcTable)')
"@
				$DsTblInsertResult = Invoke-Sqlcmd -Database $ReportingDB -ServerInstance $DbServer -Query $DsClYamlFileHdrInsQry -Username $DbUser -Password $DbPassword
			}
			
			$i1=1
			
			Foreach($varColumn in $metadata.columns)
			{
				$columnTransform= "false"
				$columnorder=$i1
			If($($varColumn).sourceColumnReferences.transform)
			{
				$columnTransform="true"
			}
            
			$varNullable= $($varColumn.nullable)  -as [string]
			$varNullable=$varNullable.ToLower()
			
	        $DsClYamlFileDtlInsQry = @"
            INSERT INTO $DsClYamlFileDtl
                   ([ClientCd]
				   ,[TblName]
                   ,[ColOrder]
                   ,[ColName]
                   ,[DataType]
                   ,[ColDescription]
                   ,[Nullable]
                   ,[ColTransform])
				  
             VALUES
                   ($ClientCd
				   ,'$($combinedObjects.steps."$stepsValue".operation.name)'
                   ,'$($i1)'
                   ,'$($varColumn.name)'
                   ,'$($varColumn.dataType)'
                   ,'$($varColumn.description)'
                   ,'$($varNullable)'
                   ,'$($columnTransform)')
"@
            $DsTblDtlInsertResult = Invoke-Sqlcmd -Database $ReportingDB -ServerInstance $DbServer -Query $DsClYamlFileDtlInsQry -Username $DbUser -Password $DbPassword
            $i1=$i1+1
            }
		}
	    }
    }

}