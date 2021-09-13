if not exist "docker" (cd ..)
if not exist "docker" (cd ..)
pipreqs ./src --force
docker build -t lizongti/stock:scheduler -f ./src/scheduler.dockerfile ./src
docker push lizongti/stock:scheduler