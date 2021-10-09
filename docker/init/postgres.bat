if not exist "docker" (cd ..)
if not exist "docker" (cd ..)


docker run --rm ^
--name postgres-updater ^
-v %cd%/docker/init/postgres:/src ^
-v presto_postgres:/dst ^
centos:latest bash -c "cp -rf /src/* /dst/"

docker service update presto_postgres --force