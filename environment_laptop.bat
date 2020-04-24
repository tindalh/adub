@echo off
setx ADUB_DBServer "DESKTOP-B595DUE\SQLEXPRESS01" /M
setx ADUB_APP "C:\dev\Analytics" /M
setx ADUB_Host localhost /M
setx ADUB_Import_Output "C:\Dev\Temp\Output" /M
setx ADUB_Import_Output_UNC "C:\Dev\Temp\Output" /M
setx ADUB_Import_Path "C:\Dev\Temp" /M
setx ADUB_Email_To "henryt@arcpet.co.uk" /M

echo ADUB_DBServer %ADUB_DBServer%
echo ADUB_APP %ADUB_APP%
echo ADUB_Host %ADUB_Host%
echo ADUB_Import_Output %ADUB_Import_Output%
echo ADUB_Import_Output_UNC %ADUB_Import_Output_UNC%
echo ADUB_Import_Path %ADUB_Import_Path%
echo ADUB_Email_To %ADUB_Email_To%

pause