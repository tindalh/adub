@echo off
setx ADUB_DBServer "arcsql\mssqlserverdev"
setx ADUB_APP "C:\Apps\Analytics" /M
setx ADUB_Host "arctargodev"
setx ADUB_Import_Output "D:\RefineryInfo\RefineryUpload\Test"
setx ADUB_Import_Output_UNC "\\arcsql\RefineryInfo\RefineryUpload\Test"
setx ADUB_Import_Path "C:\Analytics"
setx ADUB_Email_To "henryt@arcpet.co.uk" /M

echo ADUB_DBServer %ADUB_DBServer%
echo ADUB_APP %ADUB_APP%
echo ADUB_Host %ADUB_Host%
echo ADUB_Import_Output %ADUB_Import_Output%
echo ADUB_Import_Output_UNC %ADUB_Import_Output_UNC%
echo ADUB_Import_Path %ADUB_Import_Path%
echo ADUB_Email_To %ADUB_Email_To%

pause