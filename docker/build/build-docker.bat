if not exist "docker" (cd ..)
if not exist "docker" (cd ..)
pipreqs ./src --force
docker build -t lizongti/docker:stock -f ./src/dockerfile ./src
docker push lizongti/docker:stock