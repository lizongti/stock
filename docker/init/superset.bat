docker exec -it presto_superset.1.ueqaetgw1mdoppmvctt1dhe0p superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@superset.com --password admin
docker exec -it presto_superset.1.ueqaetgw1mdoppmvctt1dhe0p superset db upgrade
docker exec -it presto_superset.1.ueqaetgw1mdoppmvctt1dhe0p superset init