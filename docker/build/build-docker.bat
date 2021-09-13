if not exist "docker" (cd ..)
if not exist "docker" (cd ..)
pipreqs ./src --force
docker build -t stock -f ./src/dockerfile ./src