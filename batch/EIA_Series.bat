cd ..
cd ..
cd env\scripts
activate.bat & cd .. & cd .. & cd adub & cd batch & python run_eia.py -t "series" -s %ADUB_DBServer%  -c %ADUB_Import_Output_UNC% -b  %ADUB_Import_Output% & pause
