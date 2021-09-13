if not exist "docker" (cd ..)
if not exist "docker" (cd ..)
pipreqs ./ --force
docker build -t stock .