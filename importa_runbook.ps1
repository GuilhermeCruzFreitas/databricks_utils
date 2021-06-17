# Informa o nome dos runbooks

$runbookName = "Set-AclDataLake_new"

# Importa o RunBook

$runbookName | ForEach-Object {

    Write-Output "Importando o runbook $_" 
    $scriptPath = "$(System.DefaultWorkingDirectory)/_Analytics/arm-templates/automation/$_.ps1"
    Import-AzAutomationRunbook -Name $_ -Path $scriptPath `
    -ResourceGroupName $(resourceGroupName) -AutomationAccountName $(automationAccountName) `
    -Type PowerShell -Force -Published
}