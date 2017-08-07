rem 1w full restart
shutdown /r /t 604800
IF NOT ERRORLEVEL shutdown /r /t 0
exit /B 0
