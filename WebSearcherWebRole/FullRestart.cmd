rem 2w full restart
shutdown /r /t 1209600
IF NOT ERRORLEVEL shutdown /r /t 0
exit /B 0
