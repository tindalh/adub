IF "%ADUB_APP%" == "" GOTO NOAPP
: YESAPP
@ECHO The ADUB_APP environment variable was detected: %ADUB_APP% 

IF "%USERDOMAIN%" == "%COMPUTERNAME%" (
    SET USER=%USERNAME%
) ELSE (
    SET USER=targo.sa
)

SCHTASKS /CREATE /SC DAILY /TN "Prices\Daily Prices" /TR "%ADUB_APP%\adub\batch\ArcPrices.bat prod current" /ST 06:30 /RU "%USERDOMAIN%\%user%" /RP  /F
SCHTASKS /CREATE /SC MONTHLY /D 1 /TN "Prices\Price Returns" /TR "%ADUB_APP%\adub\batch\PriceReturns.bat" /ST 07:00  /RU "%USERDOMAIN%\%user%" /RP  /F
SCHTASKS /CREATE /SC WEEKLY /D THU /TN "Analytics\EIA Series" /TR "%ADUB_APP%\adub\batch\EIA_Series.bat" /ST 08:30 /RU "%USERDOMAIN%\%user%" /RP  /F
SCHTASKS /CREATE /SC DAILY /TN "Prices\ICE Settlement Curves" /TR "%ADUB_APP%\adub\batch\ICE_Settlement.bat" /ST 20:00 /RU "%USERDOMAIN%\%user%" /RP  /F
SCHTASKS /CREATE /SC DAILY /TN "Prices\McQuilling Assessments" /TR "%ADUB_APP%\adub\batch\McQuilling.bat" /ST 14:30 /RU "%USERDOMAIN%\%user%" /RP  /F
SCHTASKS /CREATE /SC DAILY /TN "Admin\Validate" /TR "%ADUB_APP%\adub\batch\Validate.bat" /ST 10:00 /RU "%USERDOMAIN%\%user%" /RP  /F

GOTO END
: NOAPP
@ECHO The ADUB_APP environment variable was not detected.
GOTO END
: END