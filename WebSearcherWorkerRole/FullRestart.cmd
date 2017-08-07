rem 4h full restart
shutdown /r /t 14400
IF NOT ERRORLEVEL shutdown /r /t 0
exit /B 0
