#------------------------------------------------------------------------------------------ 
#-- Function To Convert Source Objects To YAML
#------------------------------------------------------------------------------------------
Function Convert-Source-To-YAML{
	    param(
        [Parameter(Mandatory=$true)]
        [Hashtable]$Parameters,
		[Parameter(Mandatory=$true)]
        [Hashtable]$dbParameters,
		[Parameter(Mandatory=$true)]
        [Hashtable]$formParameters
    )

    #-- Set Variables
    $OneDriveAutoClClntDir = $Parameters["OneDriveAutoClClntDir"]
    $Database =$Parameters["Database"] 
    $DbServer = $Parameters["DbServer"]
    $LocDrivePsCustModDir=$Parameters["LocDrivePsCustModDir"]
    $ActionCode=$Parameters["ActionCode"]
    $userName=$Parameters["userName"]
    $password =$Parameters["password"]
    
    $DsTblHdr=$dbParameters["DsTblHdr"]
    $DsTblDtl=$dbParameters["DsTblDtl"]
    $DsTblTrl=$dbParameters["DsTblTrl"]
    $DsTblAud=$dbParameters["DsTblAud"]
    $DsTblHsh=$dbParameters["DsTblHsh"]
    $DsTblSmp=$dbParameters["DsTblSmp"]
 
    $ClientCd=$formParameters["ClientCd"] 

	
    #-- Source Objects
    try{
	$HdrTblQrySrc = "SELECT * FROM $DsTblHdr WHERE ClientCd = `'$($ClientCd)`' AND TblType = 'Source' ;"#AND TblName='ODS_CTRL_EDW';"
    $HdrTblDataSrc = Invoke-Sqlcmd -Database $Database -ServerInstance $DbServer -Query $HdrTblQrySrc -Username $userName -Password $password
	$global:resultOfStep="Success"
	}catch {
	$global:resultOfStep="Failure"
    Write-Host "Error occurred while readin from $($DsTblHdr) : $_"
    }   
	Write-Log-Message -message "Reading from  $DsTblHdr for ClientCd =`'$($ClientCd)`' AND TblType = 'Source'" -result $global:resultOfStep
	
	$YamlObjectSrc = @()
    
    ForEach($HdrTblRecordSrc in $HdrTblDataSrc)
    {  
        try{	
    	$DtlTblQrySrc = "SELECT * FROM $DsTblDtl WHERE ClientCd = `'$($ClientCd)`' AND TblId = `'$($HdrTblRecordSrc.TblId)`' Order By ColOrder;"
        $DtlTblDataSrc = Invoke-Sqlcmd -Database $Database -ServerInstance $DbServer -Query $DtlTblQrySrc -Username $userName -Password $password
        $TrlTblQrySrc = "SELECT * FROM $DsTblTrl WHERE ClientCd = `'$($ClientCd)`' AND TblId = `'$($HdrTblRecordSrc.TblId)`';"
    	$TrlTblDataSrc = Invoke-Sqlcmd -Database $Database -ServerInstance $DbServer -Query $TrlTblQrySrc -Username $userName -Password $password
    	$TrlTblGrpSrc  = $TrlTblDataSrc | Group-Object -Property TblId
        $AudTblQrySrc  = "SELECT * FROM $DsTblAud WHERE ClientCd = `'$($ClientCd)`' AND TblId = `'$($HdrTblRecordSrc.TblId)`';"
        $AudTblDataSrc = Invoke-Sqlcmd -Database $Database -ServerInstance $DbServer -Query $AudTblQrySrc -Username $userName -Password $password

    	$ColumnSrc     = @()
        $global:resultOfStep="Success"
	    }catch {
	    $global:resultOfStep="Failure"
         Write-Host "Error occurred while reading from metadata table: $_"
        }
        Write-Log-Message -message "Reading from $DsTblDtl and $DsTblTrl for  ClientCd`'$($ClientCd)`' & TblId `'$($HdrTblRecordSrc.TblId)`'" -result $global:resultOfStep
		    	
		try{
    	ForEach($DtlTblRecordSrc in $DtlTblDataSrc)
        {
            $ColumnSrc += [Ordered]@{
                acceptedValues = [Ordered]@{
                    strictMatch = $DtlTblRecordSrc.StrictMatch
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
            description = $DtlTblRecordSrc.ColDescription
            name =  $DtlTblRecordSrc.ColName
            nullable = $DtlTblRecordSrc.Nullable
            primaryKey = $DtlTblRecordSrc.PrimaryKey
            uniqueKey = $DtlTblRecordSrc.UniqueKey           
           }
        }

  try{
        ForEach($AudTblRecordSrc in $AudTblDataSrc)
        {  
            $ColumnSrc += [Ordered]@{
                  acceptedValues = [Ordered]@{
                      strictMatch = $AudTblRecordSrc.StrictMatch
                      values= @()
                }
                appliedColumnTests = [Ordered]@{}
                columnReference = [Ordered]@{
                     columnCounter =  $AudTblRecordSrc.ColId
                  stepCounter =  $AudTblRecordSrc.TblId
                 }
                config = {}
                dataType = $AudTblRecordSrc.DataType
                defaultValue=AddDoubleQuotesIfNeeded $AudTblRecordSrc.DefaultValue
                description = "$($AudTblRecordSrc.ColDescription)"
                hashedColumns = @()
                isSystemCreateDate= """"""
                name =  $AudTblRecordSrc.ColName
                nullable = $AudTblRecordSrc.Nullable
                sourceColumnReferences = [Ordered]@{
                    "columnReferences"= @()
    	            "  transform" =AddDoubleQuotesIfNeeded $AudTblRecordSrc.ColTransform
                }
            }
        }
      $global:resultOfStep="Success"
	  }catch {
	    $global:resultOfStep="Failure"
         Write-Host "Error occurred while reading from metadata table: $_"
      }
      Write-Log-Message -message "Reading from  $DsTblAud" -result $global:resultOfStep			
                
       $YamlObjectSrc = [Ordered]@{
        "$($HdrTblRecordSrc.TblName)-$($HdrTblRecordSrc.TblId)" = [Ordered]@{
            "operation" = [Ordered]@{        
            "database" = $HdrTblRecordSrc.TblDatabase
            "dataset" = """"""
            "deployEnabled" = [System.Convert]::ToBoolean($HdrTblRecordSrc.deployEnabled)
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
	  }catch {
	    $global:resultOfStep="Failure"
         Write-Host "Error occurred while reading from metadata table: $_"
      }
      Write-Log-Message -message "Reading from  $DsTblDtl and $DsTblTrl" -result $global:resultOfStep	


	   		  
      try{
	  $YamlContentSrc = $YamlObjectSrc| ConvertTo-Yaml
      $YamlContentSrc = $YamlContentSrc -Replace("---","").Trim() 
      $YamlContentSrc = "steps:`r`n  "+$YamlContentSrc -Replace( "\r\n", "`n")
      $global:resultOfStep="Success"
	    }catch {
	    $global:resultOfStep="Failure"
         Write-Host "Error occurred while converting to YAML: $_"
        }
        Write-Log-Message -message "Converting to YAML file for table $($HdrTblRecordSrc.TblName)" -result $global:resultOfStep
	  try{
	  Set-Content -Path "$($OneDriveAutoClClntDir)\$($HdrTblRecordSrc.TblType)-$($HdrTblRecordSrc.TblName)-Yaml.yml" -Value $YamlContentSrc
	  $global:resultOfStep="Success"
	   }catch {
	    $global:resultOfStep="Failure"
         Write-Host "Error occurred while writing to YAML Files: $_"
        }
        Write-Log-Message -message "Writing to $($OneDriveAutoClClntDir)\$($HdrTblRecordSrc.TblType)-$($HdrTblRecordSrc.TblName)-Yaml.yml" -result $global:resultOfStep
    }
	$global:resultOfStep="Success"
	Write-Log-Message -message "Source tables converted successfully to YAML files" -result $global:resultOfStep
}

#------------------------------------------------------------------------------------------ 
#-- Function To Convert Non-Source Objects To YAML
#------------------------------------------------------------------------------------------
Function Convert-Non-Source-To-YAML{
	    param(
        [Parameter(Mandatory=$true)]
        [Hashtable]$Parameters,
		[Parameter(Mandatory=$true)]
        [Hashtable]$dbParameters,
		[Parameter(Mandatory=$true)]
        [Hashtable]$formParameters
    )
    
	#-- Set Variables
    $OneDriveAutoClClntDir = $Parameters["OneDriveAutoClClntDir"]
    $Database =$Parameters["Database"] 
    $DbServer = $Parameters["DbServer"]
    $LocDrivePsCustModDir=$Parameters["LocDrivePsCustModDir"]
    $ActionCode=$Parameters["ActionCode"]
    $userName=$Parameters["userName"]
    $password =$Parameters["password"]

    $DsTblHdr=$dbParameters["DsTblHdr"]
    $DsTblDtl=$dbParameters["DsTblDtl"]
    $DsTblTrl=$dbParameters["DsTblTrl"]
    $DsTblAud=$dbParameters["DsTblAud"]
    $DsTblHsh=$dbParameters["DsTblHsh"]
    $DsTblSmp=$dbParameters["DsTblSmp"]
 
    $ClientCd=$formParameters["ClientCd"]   
    
    #-- Non Source (All) Objects
	try{
    $HdrTblQryAll  = "SELECT * FROM $DsTblHdr WHERE ClientCd = `'$($ClientCd)`' AND TblType <> 'Source' ;"#AND TblName='LOAD_GWC_BC_PROD_BC_PRODUCER';"
    $HdrTblDataAll = Invoke-Sqlcmd -Database $Database -ServerInstance $DbServer -Query $HdrTblQryAll -Username $userName -Password $password
	$global:resultOfStep="Success"
	}catch {
	$global:resultOfStep="Failure"
    Write-Host "Error occurred while reading from $($DsTblHdr) : $_"
    }   
	Write-Log-Message -message "Reading from  $DsTblHdr for ClientCd =`'$($ClientCd)`' AND TblType = 'Non-Source'" -result $global:resultOfStep
    $YamlObjectAll = @()
    try{
    ForEach($HdrTblRecordAll in $HdrTblDataAll)
    {  
	    try{
    	$DtlTblQryAll  = "SELECT * FROM $DsTblDtl WHERE ClientCd = `'$($ClientCd)`' AND TblId = `'$($HdrTblRecordAll.TblId)` Order By ColOrder';"
        $DtlTblDataAll = Invoke-Sqlcmd -Database $Database -ServerInstance $DbServer -Query $DtlTblQryAll -Username $userName -Password $password
		$HshTblQryAll  = "SELECT * FROM $DsTblHsh WHERE ClientCd = `'$($ClientCd)`' AND TblId = `'$($HdrTblRecordAll.TblId)`';"
        $HshTblDataAll = Invoke-Sqlcmd -Database $Database -ServerInstance $DbServer -Query $HshTblQryAll -Username $userName -Password $password
        $AudTblQryAll  = "SELECT * FROM $DsTblAud WHERE ClientCd = `'$($ClientCd)`' AND TblId = `'$($HdrTblRecordAll.TblId)`';"
        $AudTblDataAll = Invoke-Sqlcmd -Database $Database -ServerInstance $DbServer -Query $AudTblQryAll -Username $userName -Password $password
        $TrlTblQryAll  = "SELECT * FROM $DsTblTrl WHERE ClientCd = `'$($ClientCd)`' AND TblId = `'$($HdrTblRecordAll.TblId)`';"
    	$TrlTblDataAll = Invoke-Sqlcmd -Database $Database -ServerInstance $DbServer -Query $TrlTblQryAll -Username $userName -Password $password
    	$SmpTblQryAll  = "SELECT * FROM $DsTblSmp WHERE ClientCd = `'$($ClientCd)`' AND TblId = `'$($HdrTblRecordAll.TblId)`'  ORDER BY SrcTblId;"
    	$SmpTblDataAll = Invoke-Sqlcmd -Database $Database -ServerInstance $DbServer -Query $SmpTblQryAll -Username $userName -Password $password
    	$stringVariable = "'" + ($SmpTblDataAll.TblId -join "', '") + "'"						
        $SmpDtlTblQryAll  = "SELECT * FROM $DsTblDtl WHERE ClientCd = `'$($ClientCd)`' AND TblId = `'$($HdrTblRecordAll.TblId)`' AND TblId IN("+	$stringVariable+" ) ORDER BY SrcTblId,ColOrder;"
    	$SmpDtlTblDataAll = Invoke-Sqlcmd -Database $Database -ServerInstance $DbServer -Query $SmpDtlTblQryAll -Username $userName -Password $password
		$global:resultOfStep="Success"
	    }catch {
	    $global:resultOfStep="Failure"
         Write-Host "Error occurred while reading from metadata table for non source: $_"
        }
        Write-Log-Message -message "Reading from $DsTblDtl,$DsTblTrl,$DsTblHsh,$DsTblAud,$DsTblSmp for  ClientCd`'$($ClientCd)`' & TblId `'$($HdrTblRecordSrc.TblId)`'" -result $global:resultOfStep
	   $ColumnAll     = @()
       try{
	   if ($SmpDtlTblDataAll -eq $null) {
        ForEach($DtlTblRecordAll in $DtlTblDataAll)
        {      $DtlTblRecordAll.ColDescription
                $SourceColumnReference = @()
                $ColumnReference = @()
                $ColumnReference += [Ordered]@{
                                "columnCounter" = $($DtlTblRecordAll.SrcColId)
                                "stepCounter" = $DtlTblRecordAll.SrcTblId
                            }
                $SourceColumnReference += [Ordered]@{
                        "columnReferences" = $ColumnReference
    			        "transform" = AddDoubleQuotesIfNeeded $DtlTblRecordAll.ColTransform 
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
                "nullable" = $DtlTblRecordAll.nullable
                "sourceColumnReferences" = $SourceColumnReference
            }
    	  }
    	  
        }
    
      Else{
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
    			        "transform" = AddDoubleQuotesIfNeeded $Value.ColTransform 
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
				If($smt.IsBusinessKey -eq "true"){
				$varBK=$smt.IsBusinessKey
				}
				If($smt.IsChangeDetection -eq "true"){
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
                "nullable" = $smt.nullable
                "sourceColumnReferences" = $SourceColumnReference
            }		
			
         }

      $global:resultOfStep="Success"
	  }
	   }catch {
	    $global:resultOfStep="Failure"
         Write-Host "Error occurred while reading from metadata table: $_"
      }
      Write-Log-Message -message "Reading from  $DsTblDtl and $DsTblTrl" -result $global:resultOfStep		   
	   try{
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
                      strictMatch = $Hash.StrictMatch
                      values= @()
                }
                appliedColumnTests = [Ordered]@{}
                columnReference = [Ordered]@{
                    ` columnCounter =  $Hash.ColId
                  stepCounter =  $Hash.TblId
                 }
                config = {}
                dataType = $Hash.DataType
                defaultValue=AddDoubleQuotesIfNeeded $Hash.DefaultValue
                description = "$($Hash.ColDescription)"
                hashDetails = [Ordered]@{
                  hashAlgorithm = $Hash.HashAlgorithm
                }
                hashedColumns = $hashedColumns
                name =  $Hash.ColName
                nullable = $Hash.Nullable
                sourceColumnReferences = [Ordered]@{
                    "columnReferences"= @()
    				"  transform" =AddDoubleQuotesIfNeeded $Hash.ColTransform 
                }
            }
        }
       }
	  $global:resultOfStep="Success"
	  }catch {
	    $global:resultOfStep="Failure"
         Write-Host "Error occurred while reading from metadata table: $_"
      }
      Write-Log-Message -message "Reading from  $DsTblHsh and" -result $global:resultOfStep	
	   try{
        ForEach($AudTblRecordAll in $AudTblDataAll)
        {  
            $ColumnAll += [Ordered]@{
                  acceptedValues = [Ordered]@{
                      strictMatch = $AudTblRecordAll.StrictMatch
                      values= @()
                }
                appliedColumnTests = [Ordered]@{}
                columnReference = [Ordered]@{
                     columnCounter =  $AudTblRecordAll.ColId
                  stepCounter =  $AudTblRecordAll.TblId
                 }
                config = {}
                dataType = $AudTblRecordAll.DataType
                defaultValue=AddDoubleQuotesIfNeeded $AudTblRecordAll.DefaultValue
                description = "$($AudTblRecordAll.ColDescription)"
                hashedColumns = @()
                isSystemCreateDate= """"""
                name =  $AudTblRecordAll.ColName
                nullable = $AudTblRecordAll.Nullable
                sourceColumnReferences = [Ordered]@{
                    "columnReferences"= @()
    	            "  transform" =AddDoubleQuotesIfNeeded $AudTblRecordAll.ColTransform
                }
            }
        }
      $global:resultOfStep="Success"
	  }catch {
	    $global:resultOfStep="Failure"
         Write-Host "Error occurred while reading from metadata table: $_"
      }
      Write-Log-Message -message "Reading from  $DsTblAud" -result $global:resultOfStep	
	   		
      try{
        $SmpTblGrpAll = $SmpTblDataAll | Group-Object -Property TblId
        ForEach($SmpTblRecordAll in $SmpTblGrpAll)
        {

               $sourceMapping= @()	
    		   $joincondition= @()
    		   $locationName = @()
    		   $dependencies = @()
    		   $nolinkrefs=@()    		   
    		   for($i=0;$i -lt $SmpTblRecordAll.Group.Count ;$i++){
    		   if ($SmpTblRecordAll.Group[$i].DepNodeName) {
               $dependencies +=[Ordered]@{		
                "locationName" = $SmpTblRecordAll.Group[$i].DepLocName
                nodeName =  $SmpTblRecordAll.Group[$i].DepNodeName
               }
    		   }
    
    		   if ($SmpTblRecordAll.Group[$i].NoLinkLocName) {
               $nolinkrefs +=[Ordered]@{		
                "locationName" = $SmpTblRecordAll.Group[$i].NoLinkLocName
                nodeName =  $SmpTblRecordAll.Group[$i].NoLinkNodeName
               }
    		   }
    		   else{
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
	  }catch {
	    $global:resultOfStep="Failure"
         Write-Host "Error occurred while reading from metadata table: $_"
      }
      Write-Log-Message -message "Reading from  $DsTblSmp" -result $global:resultOfStep	
	   
       $TrlTblGrpAll = $TrlTblDataAll | Group-Object -Property TblId		  
	   try{
       if ($TrlTblDataAll -ne $null) {
        $YamlObjectAll = [Ordered]@{
         "$($HdrTblRecordAll.TblName)-$($HdrTblRecordAll.TblId)" = [Ordered]@{
             "operation" = [Ordered]@{
             "config" = [Ordered]@{
                 "insertStrategy" = $HdrTblRecordAll.InsertStrategy
                 "postSQL" = "$($HdrTblRecordAll.PostSql)" 
                 "preSQL" = "$($HdrTblRecordAll.PreSql)"                      
                 "testsEnabled" = [System.Convert]::ToBoolean($HdrTblRecordAll.TestsEnabled)
                 "truncateBefore" =[System.Convert]::ToBoolean($HdrTblRecordAll.truncateBefore)
             }
             "database" = $HdrTblRecordAll.TblDatabase
             "deployEnabled" = [System.Convert]::ToBoolean($HdrTblRecordAll.deployEnabled)
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
	  }catch {
	    $global:resultOfStep="Failure"
         Write-Host "Error occurred while reading from metadata table: $_"
      }
      Write-Log-Message -message "Reading from  $DsTblDtl and $DsTblTrl" -result $global:resultOfStep	
	   
	try{
    $YamlContentAll =$YamlObjectAll  | ConvertTo-Yaml 
    $YamlContentAll = $YamlContentAll -Replace("---","").Trim() 
    $YamlContentAll = "steps:`r`n  "+$YamlContentAll -Replace( "\r\n", "`n")
	$global:resultOfStep="Success"
	}catch {
	$global:resultOfStep="Failure"
    Write-Host "Error occurred while converting to YAML: $_"
    }
    Write-Log-Message -message "Converting to YAML file for table $($HdrTblRecordAll.TblName)" -result $global:resultOfStep
     try{
    Set-Content -Path "$($OneDriveAutoClClntDir)\$($HdrTblRecordAll.TblType)-$($HdrTblRecordAll.TblName)-Yaml.yml" -Value $YamlContentAll
     $global:resultOfStep="Success"
	   }catch {
	    $global:resultOfStep="Failure"
         Write-Host "Error occurred while writing to YAML Files: $_"
        }
        Write-Log-Message -message "Writing to $($OneDriveAutoClClntDir)\$($HdrTblRecordAll.TblType)-$($HdrTblRecordAll.TblName)-Yaml.yml" -result $global:resultOfStep
    }  
	$global:resultOfStep="Success"
	Write-Log-Message -message "Non Source tables converted successfully to YAML files" -result $global:resultOfStep
	}catch {
	$global:resultOfStep="Failure"
    Write-Host "Error occurred while converting to YAML: $_"
    }
}

#------------------------------------------------------------------------------------------ 
#-- Function To Read Generated YAML Files And Write To Database Validation Tables
#------------------------------------------------------------------------------------------
Function Validate-YAML-File{
	    param(
        [Parameter(Mandatory=$true)]
        [Hashtable]$Parameters,
		[Parameter(Mandatory=$true)]
        [Hashtable]$dbParameters
    )
    #-- Set Variables  
    $OneDriveAutoClClntDir = $Parameters["OneDriveAutoClClntDir"]
    $Database =$Parameters["Database"] 
    $DbServer = $Parameters["DbServer"]
    $LocDrivePsCustModDir=$Parameters["LocDrivePsCustModDir"]
    $ActionCode=$Parameters["ActionCode"]
    $userName=$Parameters["userName"]
    $password =$Parameters["password"]

    $DsTblReporting=$dbParameters["DsMigrTblReporting"]
    $DsTblDtlReporting=$dbParameters["DsMigrTblDtlReporting"]

$Database = "DE_TEST_DB"
$DbServer = "Poonam\SQLEXPRESS"#"34.173.131.246"
$LocDrivePsCustModDir="D:\Coalesce\PRJ2003\Powershell Modules"
$userName="de_migr_admin"
$password ="Migration@2024"

$yamlFileLocation="C:\Temp\nodes\"
$DsTblReporting="dbo.DsMigrTblReporting"
$DsTblDtlReporting="dbo.DsMigrTblDtlReporting"
 $TruncateDtl="TRUNCATE TABLE [DE_TEST_DB].[dbo].[DsMigrTblDtlReporting]"
 $TruncateDtl = Invoke-Sqlcmd -Database $Database -ServerInstance $DbServer -Query $TruncateDtl -Username $userName -Password $password
 $TruncateTbl="TRUNCATE TABLE [DE_TEST_DB].[dbo].[DsMigrTblReporting]"
 $TruncateTbl= Invoke-Sqlcmd -Database $Database -ServerInstance $DbServer -Query $TruncateTbl -Username $userName -Password $password
if (Test-Path -Path $yamlFileLocation -PathType Container) {

   $yamlFiles = Get-ChildItem -Path $yamlFileLocation -Filter *.yml | Sort-Object
    if ($yamlFiles.Count -gt 0) {
        for ($i = 0; $i -lt $yamlFiles.Count; $i++) {
            $yamlFile = $yamlFiles[$i]  
            $yamlFileRead = Get-Content -Path $yamlFile.FullName
			$yamlFile.FullName
            $combinedObjects = $yamlFileRead | ConvertFrom-Yaml
            $stepsValue = $combinedObjects.steps.Keys
            $metadata = $combinedObjects.steps."$stepsValue".operation.metadata
			$varIsMultiSource=$($combinedObjects.steps."$stepsValue".operation.isMultisource) -as [string]
			$varIsMultiSource=$varIsMultiSource.ToLower()
			if($($varIsMultiSource) -eq 'true')
			{
			$multiTable=$metadata.sourceMapping.name
			}
			else{
			$multiTable=@("")
			$varIsMultiSource='false'
			}
			$varTblDescription="false"
			if($($combinedObjects.steps."$stepsValue".operation.description)){
				$varTblDescription="true"
			}
	        foreach($srcTable in $multiTable){
			$DsTblInsert = @"
	        INSERT INTO [dbo].[DsMigrTblReporting]
                   ([TblName]
                   ,[TblDatabase]
                   ,[TblDescription]
                   ,[IsMultiSource]
                   ,[LocationName]
                   ,[SmpTblName])
             VALUES
                   ('$($combinedObjects.steps."$stepsValue".operation.name)'
                   ,'$($combinedObjects.steps."$stepsValue".operation.database)'
                   ,'$($varTblDescription)'
                   ,'$($varIsMultiSource)'
                   ,'$($combinedObjects.steps."$stepsValue".operation.locationName)'
                   ,'$($srcTable)')
"@
            $DsTblInsertResult = Invoke-Sqlcmd -Database $Database -ServerInstance $DbServer -Query $DsTblInsert -Username $userName -Password $password
			}
			$i1=1
			foreach($varColumn in $metadata.columns){
			$columnTransform= "false"
            $columnorder=$i1
			if($metadata.columns[$i1].sourceColumnReferences.transform){
				$columnTransform="true"
			}
            $varNullable= $($varColumn.nullable)  -as [string]
			$varNullable=$varNullable.ToLower()
	        $DsTblDtlInsert = @"
            INSERT INTO [dbo].[DsMigrTblDtlReporting]
                   ([TblName]
                   ,[ColOrder]
                   ,[ColName]
                   ,[DataType]
                   ,[ColDescription]
                   ,[Nullable]
                   ,[ColTransform])
				  
             VALUES
                   ('$($combinedObjects.steps."$stepsValue".operation.name)'
                   ,'$($i1)'
                   ,'$($varColumn.name)'
                   ,'$($varColumn.dataType)'
                   ,'$($varColumn.description)'
                   ,'$($varNullable)'
                   ,'$($columnTransform)')
"@
            $DsTblDtlInsertResult = Invoke-Sqlcmd -Database $Database -ServerInstance $DbServer -Query $DsTblDtlInsert -Username $userName -Password $password
            $i1=$i1+1
            }
		}
	}
}

}