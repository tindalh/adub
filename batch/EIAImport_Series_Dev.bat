@echo off

cd ..
cd ..
cd env\scripts
activate.bat & cd .. & cd .. & cd adub & python runEia.py -t "series" -s "lon-pc53" -i %1 -c "C:\Dev\Excel Files\EIA" -b  "C:\Dev\Excel Files\EIA" & pause
