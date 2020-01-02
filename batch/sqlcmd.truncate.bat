
@echo off
IF %1.==. GOTO No1
IF %2.==. GOTO No2

sqlcmd -S %1 -E -Q "TRUNCATE TABLE %2"
GOTO End1

:No1
  ECHO No server name and instance supplied (e.g. "arcsql.arcpet.co.uk\mssqlserverdev")
GOTO Error
:No2
  ECHO No database.schema.table name supplied (e.g. "Price.import.ICE_L1_1_Current")
GOTO Error

:End1
exit 0
:Error
exit 1


