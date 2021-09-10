docker exec -it hive_superset superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@superset.com --password admin
docker exec -it hive_superset superset db upgrade
docker exec -it hive_superset superset init