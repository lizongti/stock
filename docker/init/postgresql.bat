if not exist "docker" (cd ..)
if not exist "docker" (cd ..)


docker run --rm ^
--name postgresql-updater ^
-v %cd%/docker/init/postgresql:/src ^
-v presto_postgresql:/dst ^
centos:latest bash -c "cp -rf /src/* /dst/"

docker service update presto_pika --force