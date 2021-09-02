if not exist "scripts" (cd ..)

docker run --rm ^
--name presto-updater ^
-v presto_conf:/opt/presto_conf ^
-v %cd%/presto:/opt/presto ^
centos:latest bash -c "cp -rf /opt/presto/* /opt/presto_conf"

docker restart presto
docker logs presto