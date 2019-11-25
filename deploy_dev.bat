cd c:\Dev\Projects\Analytics\Python37
robocopy . c:\Apps\Analytics /MIR  /xf *deploy*.bat test.py .gitignore *_prod* *_test* /xd .git\ __pycache__\ devops\ env\

sc stop adub
pause
sc start adub

pause