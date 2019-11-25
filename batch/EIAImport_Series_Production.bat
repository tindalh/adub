@echo off

cd ..
cd ..
cd env\scripts
activate.bat & cd .. & cd .. & cd adub & python runEia.py -t "series" -s "arcsql" -i %1 -c "\\arcsql\RefineryInfo\RefineryUpload\EIA" -b  "D:\RefineryInfo\RefineryUpload\EIA" & pause
