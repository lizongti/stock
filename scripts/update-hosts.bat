:: Test of environment variable length
@echo off & setlocal EnableDelayedExpansion
REM 将要添加的域名都写在这里，用;号隔开
set strp=172.169.18.101 omen;172.169.18.101 omen4;172.169.18.101 pika;172.169.18.101 presto

set hostsfile="%SystemRoot%\system32\drivers\etc\hosts"

:for
for /F "delims=; tokens=1,*" %%A in ("!strp!") do (


REM 取得第一个Host
set stHosts=%%A
REM echo A = !stHosts!


REM 取得剩余的Host
set strp=%%B
REM echo B = !strp!


REM 设置插入标记true false
set ins=true


FOR /F "eol=# tokens=1 usebackq delims=" %%i in (%hostsfile%) do if "!stHosts!"=="%%i" set ins=flase
if "!ins!"=="true" echo !stHosts!>> %hostsfile%


)


REM echo B-EOF: = !strp!
REM 判断变量是否为空，不为空就循环提前。 
if not "!strp!"=="" goto :for

@ipconfig /flushdns
@echo
@pause > nul
@exit