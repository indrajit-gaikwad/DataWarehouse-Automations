#------------------------------------------------------------------------------------------ 
#-- READ PARAMETERS FILE
#------------------------------------------------------------------------------------------
Function Read-Parameters-File
{
 	Param
	(
		[String]$ParametersFile
    )   
    try
	{
		# Read parameter file into hash table
		$ParamsContent = Get-Content $ParametersFile -Raw
		$ParamsHashTable = $ParamsContent | ConvertFrom-StringData
		$Global:ResultOfStep="Success" 
	}catch 
	{
		$Global:ResultOfStep="Failure"
		Write-Host "Error occurred while writing to log file: $_"
    }
	Return $ParamsHashTable
}

#---------------------------------------------------------------------------------------------------------
# Function: Send Email Uisng Gmail
#---------------------------------------------------------------------------------------------------------
Function Send-Email-Gmail
{
    param
	(
		[string]$EmailSubject,
        [string]$EmailBody,
		[string]$RecipientEmail,
		[string]$AttachFileFlag,
		[string]$EmailAttach
    )	
	$UserName = 'dataentrega.automations@gmail.com'
	$Password = 'haiymealustzgzpn'
	[SecureString]$SecurePassword = $Password | ConvertTo-SecureString -AsPlainText -Force
	$Credential = New-Object System.Management.Automation.PSCredential -ArgumentList $UserName, $SecurePassword
	If ($AttachFileFlag -eq "Y")
	{
		Send-MailMessage -SmtpServer smtp.gmail.com -Port 587 -UseSsl -Credential $Credential -From $UserName -To $RecipientEmail -Subject $EmailSubject -Body $EmailBody -BodyAsHtml -Attachments $EmailAttach
	}
	Else
	{
		Send-MailMessage -SmtpServer smtp.gmail.com -Port 587 -UseSsl -Credential $Credential -From $UserName -To $RecipientEmail -Subject $EmailSubject -Body $EmailBody -BodyAsHtml
	}
	
}

#---------------------------------------------------------------------------------------------------------
# Write-Log-Message: Write Log Message To Console
#---------------------------------------------------------------------------------------------------------
Function Write-Log-Message 
{
    param
	(
		[string]$message,
		[string]$result
	)
	
	Write-Host ("Step    Timestamp                  Result        Message")
	Write-Host ("------------------------------------------------------------------------------------------------------------------")
	$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
	$global:stepCounter=$global:stepCounter+1
    $logMessage = " $global:stepCounter     $timestamp         $result       $message"
	$logMessage 
	Write-Host ("`n")
	try 
	{
		Add-Content -Path $global:logFile -Value $logMessage
    } catch 
	{
		Write-Host "Error occurred while writing to log file: $_"
    }
}

#---------------------------------------------------------------------------------------------------------
# Add-Double-Quotes-To-String: Add Double Quotes To String
#---------------------------------------------------------------------------------------------------------
Function Add-Double-Quotes-To-String
{
    param 
	(
        [string]$InString
    )

    If ($InString.StartsWith("'") -and $InString.EndsWith("'")) 
	{
        Return "`"$InString`""
    }
    Else 
	{
        Return $InString
    }
}