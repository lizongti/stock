if not exist "scripts" (cd ..)

docker run --rm ^
--name presto-updater ^
-v %cd%/presto:/src ^
-v hive_presto:/dst ^
centos:latest bash -c "rm -rf /dst/* && cp -rf /src/* /dst/"

docker service update hive_presto --force