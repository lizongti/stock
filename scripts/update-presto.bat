if not exist "scripts" (cd ..)

docker run --rm ^
--name presto-updater ^
-v %cd%/presto-conf:/opt/presto-conf-src ^
-v hive_presto-conf:/opt/presto-conf-dst ^
centos:latest bash -c "cp -rf /opt/presto-conf-src/* /opt/presto-conf-dst"

docker service update hive_presto-coordinator