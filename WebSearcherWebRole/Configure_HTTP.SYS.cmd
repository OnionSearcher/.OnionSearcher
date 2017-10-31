setlocal
set regpath=HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\services\HTTP\Parameters
reg query "%regpath%" /v "DisableServerHeader"
if errorlevel 1 (
   reg add %regpath% /v DisableServerHeader /t REG_DWORD /d 00000001
   shutdown /r /t 0
)

%windir%\system32\inetsrv\appcmd set config -section:applicationPools -applicationPoolDefaults.processModel.idleTimeout:04:00:00
%windir%\system32\inetsrv\appcmd set config -section:applicationPools -applicationPoolDefaults.processModel.maxProcesses:2
%windir%\system32\inetsrv\appcmd set config -section:applicationPools -applicationPoolDefaults.startMode:AlwaysRunning
