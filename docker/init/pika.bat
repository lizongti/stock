if not exist "scripts" (cd ..)

docker run --rm ^
--name pika-updater ^
-v %cd%/pika:/src ^
-v hive_pika-conf:/dst ^
centos:latest bash -c "rm -rf /dst/* && cp -rf /src/* /dst/"

docker service update hive_pika --force