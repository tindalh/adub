@echo off

cd ..
cd ..
cd env\scripts
activate.bat & cd .. & cd .. & cd adub & cd batch & python run_eia.py -t "series" -s "arcsql\mssqlserverdev" -i %1 -c "\\arcsql\RefineryInfo\RefineryUpload\EIA" -b  "D:\RefineryInfo\RefineryUpload\EIA" & pause
