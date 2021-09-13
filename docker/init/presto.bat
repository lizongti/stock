if not exist "docker" (cd ..)
if not exist "docker" (cd ..)

docker run --rm ^
--name presto-updater ^
-v %cd%/docker/init/presto:/src ^
-v presto_presto:/dst ^
centos:latest bash -c "rm -rf /dst/* && cp -rf /src/* /dst/"

docker service update presto_presto --force