
@echo off
IF %1.==. GOTO No1
IF %2.==. GOTO No2
IF %3.==. GOTO No3

BCP %1 format nul -c -f %2 -S %3 -t -T 
GOTO End1

:No1
  ECHO No source database.schema.table name supplied (e.g. "ARC_DOCDROP.dbo.TEMP_LANDED_MFL_L1_ICE_1")
GOTO End1
:No2
  ECHO No format file path supplied (e.g. "c:\dev\projects\price\data\format.TEMP_LANDED_MFL_L1_ICE_1.fmt")
GOTO End1
:No3
  ECHO No source server name and instance supplied (e.g. "ARCBO8\DEV")
GOTO End1

:End1