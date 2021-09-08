if not exist "scripts" (cd ..)

docker run --rm ^
--name presto-updater ^
-v %cd%/presto:/src/presto ^
-v hive_presto:/dst/presto ^
centos:latest bash -c "cp -rf /src/presto/* /dst/presto/"

docker service update hive_presto