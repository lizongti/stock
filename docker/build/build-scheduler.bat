if not exist "docker" (cd ..)
if not exist "docker" (cd ..)
pipreqs ./src --encoding=utf-8 --force
docker build -t lizongti/stock:scheduler -f ./src/scheduler.dockerfile ./src
docker push lizongti/stock:scheduler 