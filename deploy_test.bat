cd c:\Dev\Projects\Analytics\Python37
robocopy C:\Dev\Projects\Analytics\Python37 \\arctargodev\apps2\Analytics /MIR  /xf *deploy*.bat test.py .gitignore *_dev* *_prod* /xd .git\ __pycache__\ devops\ env\


sc \\arctargodev stop ad
pause
sc \\arctargodev start ad


pause