#------------------------------------------------------------------------------------------ 
# Script	: 00-Automations-Main
# Author	: Indrajit Gaikwad
# Date  	: 14-APR-2024
# Purpose	: Automations Main Script
#------------------------------------------------------------------------------------------ 


#------------------------------------------------------------------------------------------ 
#-- IMPORT REQUIRED MODULES
#------------------------------------------------------------------------------------------
Import-Module -Name Powershell-Yaml
Import-Module -FullyQualifiedName "$env:USERPROFILE\DE-Automations\PowerShell\Modules\DEA-PS-CommonModule.psm1" -DisableNameChecking
Import-Module -FullyQualifiedName "$env:USERPROFILE\DE-Automations\PowerShell\Modules\DEA-PS-CL-Repo-To-YAML.psm1" -DisableNameChecking
Import-Module -FullyQualifiedName "$env:USERPROFILE\DE-Automations\PowerShell\Modules\DEA-PS-WS-Repo-To-CL-Repo.psm1" -DisableNameChecking
Import-Module -FullyQualifiedName "$env:USERPROFILE\DE-Automations\PowerShell\Modules\PSYaml\PSYaml.psm1" -DisableNameChecking

#------------------------------------------------------------------------------------------ 
#-- SET GLOBAL VARIABLES TO GENERATE LOGS
#------------------------------------------------------------------------------------------
$Global:StepCounter=0
$Global:LogFile=""
$Global:ResultOfStep=""

#------------------------------------------------------------------------------------------ 
#-- GET PARAMETERS
#------------------------------------------------------------------------------------------
#-- User Parameters
$UserParametersFile          = "$env:USERPROFILE\DE-Automations\Parameters\DEA-PS-UserParameters.prm"
$UserParametersHashTable     = Read-Parameters-File -ParametersFile $UserParametersFile

#------------------------------------------------------------------------------------------ 
#-- SET FILE PATHS
#------------------------------------------------------------------------------------------

$AutomationsDirectory   = $UserParametersHashTable.AutomationsDirectory
$ODAutomationsDirectory = $UserParametersHashTable.OneDriveAutomationsDirectory

#------------------------------------------------------------------------------------------ 
#-- FORM PARAMETERS FILE
#------------------------------------------------------------------------------------------
#-- Read File & Assign Variables
$Parameters       = (Get-Content ${ODAutomationsDirectory}\01-Automations.prm).Split("|")
$LogId            = $Parameters[0]
$ClientCode       = $Parameters[1]
$RecipientEmail   = $Parameters[2]
$ServerType       = $Parameters[3]
$Source           = $Parameters[4]
If ($Source -like "*WhereScape*")
{
    $SourceDB     = $Parameters[5]
    $ClientRepoDB = $Parameters[6]
    $SourceObject = $Parameters[7]
}
$Target           = $Parameters[8]
If ($Target -like "*Coalesce*")
{
    $TargetDB     = $Parameters[9]
}
$ProjectName      = $Parameters[10]
$WorkSpace        = $Parameters[11]


#-- Check If Client Directory Exists Else Create It
If (!(Test-Path ${AutomationsDirectory}\${ClientCode} -PathType Container))
{
	New-Item -ItemType Directory -Force -Path ${AutomationsDirectory}\${ClientCode}
}
		
#-- Check If Current LogID Directory Exists Else Create It
If (!(Test-Path ${AutomationsDirectory}\${ClientCode}\${LogID} -PathType Container))
{
	New-Item -ItemType Directory -Force -Path ${AutomationsDirectory}\${ClientCode}\${LogID}
}		


#-- Archive Original Parameters File
Move-Item -Path ${ODAutomationsDirectory}\01-Automations.prm -Destination "${AutomationsDirectory}\${ClientCode}\${LogID}\01-Automations.prm" -Force

#-- Create Forms Parameters File In Required Format For Hash Table
$FormParametersFile = "${AutomationsDirectory}\${ClientCode}\${LogID}\DEA-PS-FormParameters.prm"

"LogId=${LogId}" | Out-File -FilePath $FormParametersFile
"ClientCode=${ClientCode}" | Out-File -FilePath $FormParametersFile -Append
"RecipientEmail=${RecipientEmail}" | Out-File -FilePath $FormParametersFile -Append
"ServerType=${ServerType}" | Out-File -FilePath $FormParametersFile -Append
"Source=${Source}" | Out-File -FilePath $FormParametersFile -Append
"SourceDB=${SourceDB}" | Out-File -FilePath $FormParametersFile -Append
"ClientRepoDB=${ClientRepoDB}" | Out-File -FilePath $FormParametersFile -Append
"SourceObject=${SourceObject}" | Out-File -FilePath $FormParametersFile -Append
"Target=${Target}" | Out-File -FilePath $FormParametersFile -Append
"TargetDB=${TargetDB}" | Out-File -FilePath $FormParametersFile -Append
"ProjectName=${ProjectName}" | Out-File -FilePath $FormParametersFile -Append
"WorkSpace=${WorkSpace}" | Out-File -FilePath $FormParametersFile -Append

#-- Form Parameters
$FormParametersHashTable     = Read-Parameters-File -ParametersFile $FormParametersFile


#------------------------------------------------------------------------------------------ 
#-- DELETE TRIGGER FILE
#------------------------------------------------------------------------------------------
If (Test-Path -Path ${ODAutomationsDirectory}\01-Automations-Trigger)
{
	Remove-Item ${ODAutomationsDirectory}\01-Automations-Trigger
}

#------------------------------------------------------------------------------------------ 
#-- EXECUTE THE MODULES BASED ON OPERATION
#------------------------------------------------------------------------------------------
If ($OperationCode -eq "201")
{
    #-- Update Status (Power Automate)
	Send-Email-Gmail "$LogId-$Target-Staging-0" "Trigger Email" $AutomationsEmail "N"
    
    #-- Convert WhereScape Repository To Coalesce
    Convert-WS-To-CL -UserParametersHashTable $UserParametersHashTable -FormParametersHashTable $FormParametersHashTable
    
  	#-- Update Status (Power Automate)
	Send-Email-Gmail "$LogId-$Target-Processing Target (YAML Files)-0" "Trigger Email" $AutomationsEmail "N"
    
    #-- Generate YAML Files (Source Objects) From Coalesce Repository
    Convert-Source-To-YAML -UserParametersHashTable $UserParametersHashTable -FormParametersHashTable $FormParametersHashTable
    
    #-- Generate YAML Files (Non-Source Objects) From Coalesce Repository
    Convert-Non-Source-To-YAML -UserParametersHashTable $UserParametersHashTable -FormParametersHashTable $FormParametersHashTable
    
    #-- Update Status (Power Automate)
	Send-Email-Gmail "$LogId-$Target-Validation-0" "Trigger Email" $AutomationsEmail "N"
    
    #-- YAML File Validation
    Validate-YAML-File -UserParametersHashTable $UserParametersHashTable -FormParametersHashTable $FormParametersHashTable
}
Else
{
    $ExitStatusCode = "0001"
	#-- Send A Trigger Email For PowerAutomate
	Send-Email-Gmail "$LogId-9999-$ExitStatusCode" "Trigger Email" $AutomationsEmail "N"
    #-- Exit
	Exit
}

#------------------------------------------------------------------------------------------ 
#-- FINAL STEP: PASS CONTROL TO POWER AUTOMATE TO SEND EMAIL & REPORT
#------------------------------------------------------------------------------------------
#-- Send A Trigger Email For PowerAutomate
Send-Email-Gmail "$LogId-$Target-Complete-0" "Trigger Email" $AutomationsEmail "N"

#-- Wait 60 Seconds For Power Automate To Process
Start-Sleep -Seconds 60