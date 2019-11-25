cd c:\Dev\Projects\Analytics\Python37
robocopy . \\arctargoprod\Apps\Analytics /MIR  /xf *deploy*.bat test.py .gitignore *_dev* *_test* /xd .git\ __pycache__\ devops\ env\


sc \\arctargoprod stop adub
pause
sc \\arctargoprod start adub


pause