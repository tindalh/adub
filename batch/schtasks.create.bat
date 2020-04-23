IF "%ADUB_APP%" == "" GOTO NOAPP
: YESAPP
@ECHO The ADUB_APP environment variable was detected: %ADUB_APP% 

SCHTASKS /DELETE /TN "Prices\Daily Prices"
SCHTASKS /DELETE /TN "Analytics\EIA Series"
SCHTASKS /DELETE /TN "Prices\ICE Settlement Curves"
SCHTASKS /DELETE /TN "Prices\McQuilling Assessments"
SCHTASKS /DELETE /TN "Admin\Validate"

SCHTASKS /CREATE /SC DAILY /TN "Prices\Daily Prices" /TR "%ADUB_APP%\adub\batch\ArcPrices.bat prod current" /ST 06:30
SCHTASKS /CREATE /SC WEEKLY /D THU /TN "Analytics\EIA Series" /TR "%ADUB_APP%\adub\batch\EIA_Series.bat" /ST 08:30
SCHTASKS /CREATE /SC DAILY /TN "Prices\ICE Settlement Curves" /TR "%ADUB_APP%\adub\batch\ICE_Settlement.bat" /ST 20:00
SCHTASKS /CREATE /SC DAILY /TN "Prices\McQuilling Assessments" /TR "%ADUB_APP%\adub\batch\McQuilling.bat" /ST 14:30
SCHTASKS /CREATE /SC DAILY /TN "Admin\Validate" /TR "%ADUB_APP%\adub\batch\Validate.bat" /ST 10:00

GOTO END
: NOAPP
@ECHO The ADUB_APP environment variable was not detected.
GOTO END
: END