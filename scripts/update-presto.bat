if not exist "scripts" (cd ..)

docker run --rm ^
--name presto-updater ^
-v presto_catalog:/opt/presto_catalog ^
-v presto_tables:/opt/presto_tables ^
-v %cd%/presto:/opt/presto ^
centos:latest bash -c "cp -rf /opt/presto/catalog/* /opt/presto_catalog/ && cp -rf /opt/presto/tables/* /opt/presto_tables/"

docker restart presto
docker logs presto