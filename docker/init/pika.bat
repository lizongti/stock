if not exist "docker" (cd ..)
if not exist "docker" (cd ..)

docker run --rm ^
--name pika-updater ^
-v %cd%/docker/init/pika:/src ^
-v presto_pika-conf:/dst ^
centos:latest bash -c "rm -rf /dst/* && cp -rf /src/* /dst/"

docker service update presto_pika --force