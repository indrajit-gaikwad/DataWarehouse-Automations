#------------------------------------------------------------------------------------------ 
# Script	: 00-File-Watcher
# Author	: Indrajit Gaikwad
# Date  	: 15-AUG-2023
# Purpose	: Watch For Trigger Files & Execute Associated Script
#------------------------------------------------------------------------------------------ 

#------------------------------------------------------------------------------------------ 
#-- SET FILE PATHS
#------------------------------------------------------------------------------------------
#-- One Drive Paths
$OneDriveAutoMainDir   = "W:\O365\OneDrive - Data Entrega India Pvt Ltd\DNS-DE-Automations"
$OneDriveAutoInfileDir = "${OneDriveAutoMainDir}\InFiles"

#--Local Drive Paths
$LocDrivePsScriptsDir = "W:\PowerShell\Scripts"

#------------------------------------------------------------------------------------------ 
#-- CHECK FOR THE TRIGGER FILES IF ANY
#------------------------------------------------------------------------------------------
#-- Set Interval (Seconds)
$IntervalSeconds = 60

#-- Check If Any File Exists
While ($True)
{
	#-- Get Filenames Into An Array
    $FileNames = (Get-ChildItem -Path "${OneDriveAutoInfileDir}\*.trg" | Select-Object Name).Name

    If ($FileNames.Count -eq 0)
	{
		Write-Host "No Files Found."
		Write-Host "Check After $IntervalSeconds Seconds For Next Trigger File...."
		Start-Sleep -Seconds $IntervalSeconds
	} ElseIf ($FileNames.Count -eq 1)
	{
		$File           = $FileNames
		$SuccScriptName = $File.Replace('trg','ps1')
		Write-Host "File $FileNames Found."
		Write-Host "Trigger File Name  : $File"
		$CommandLine = "-File ${LocDrivePsScriptsDir}\${SuccScriptName}.ps1"
        Start-Process Powershell.exe -ArgumentList $CommandLine -Wait
		Write-Host "Called Script Name : ${LocDrivePsScriptsDir}\${SuccScriptName}"
		Write-Host ""
		Write-Host "Check After $IntervalSeconds Seconds For Next Trigger File...."
		Write-Host ""
		Start-Sleep -Seconds $IntervalSeconds
	} Else
	{
		$File           = $FileNames[0]
		$SuccScriptName = $File.Replace('trg','ps1')
		Write-Host "File $FileNames[0] Found."
		Write-Host $File
		$CommandLine = "-File ${LocDrivePsScriptsDir}\${SuccScriptName}.ps1"
		Write-Host "Called Script Name : ${LocDrivePsScriptsDir}\${SuccScriptName}"
		Write-Host ""
		Write-Host "Check After $IntervalSeconds Seconds For Next Trigger File...."
		Write-Host ""
		Start-Sleep -Seconds $IntervalSeconds		
	}
}

