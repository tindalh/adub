
@echo off
IF %1.==. GOTO No1
IF %2.==. GOTO No2
IF %3.==. GOTO No3
IF %4.==. GOTO No4

BCP %1 out %2 -f %3 -S %4 -T 
GOTO End1

:No1
  ECHO No source database.schema.table name supplied (e.g. "ARC_DOCDROP.dbo.TEMP_LANDED_MFL_L1_ICE_1")
GOTO Error
:No2
  ECHO No target data file path supplied (e.g. "c:\dev\projects\price\data\data.TEMP_LANDED_MFL_L1_ICE_1.txt")
GOTO Error
:No3
  ECHO No format file path supplied (e.g. "c:\dev\projects\price\data\format.TEMP_LANDED_MFL_L1_ICE_1.fmt")
GOTO Error
:No4
  ECHO No source server name and instance supplied (e.g. "ARCBO8\DEV")
  
GOTO Error

:End1
exit 0
:Error
exit 1


