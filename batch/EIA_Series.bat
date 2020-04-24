%ADUB_APP%\env\scripts\activate.bat & cd %ADUB_APP%\adub\batch & python run_eia.py -t "series" -s %ADUB_DBServer%  -c %ADUB_Import_Output_UNC% -b  %ADUB_Import_Output%
