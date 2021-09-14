if not exist "docker" (cd ..)
if not exist "docker" (cd ..)
docker build --no-cache -t lizongti/stock:scheduler -f ./src/scheduler.dockerfile ./src
docker push lizongti/stock:scheduler 