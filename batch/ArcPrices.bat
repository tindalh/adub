@echo off
IF %1.==. GOTO No1
IF %2.==. GOTO No2

cd ..
cd ..
cd env\scripts
activate.bat & cd .. & cd .. & cd adub & cd importers & python arcPrices.py --env %1 --data %2
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
exit 1