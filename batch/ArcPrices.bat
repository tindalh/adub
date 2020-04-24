@echo off
IF %1.==. GOTO No1
IF %2.==. GOTO No2

%ADUB_APP%\env\scripts\activate.bat & cd %ADUB_APP%\adub\importers & python arcPrices.py --env %1 --data %2
GOTO End

:No1
  ECHO No environment name supplied (i.e. "dev, test, prod")
GOTO Error
:No2
  ECHO No time span data (i.e. "current, history")
GOTO Error

:End
exit 0
:Error
pause
exit 1