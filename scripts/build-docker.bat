if not exist "scripts" (cd ..)
pipreqs ./ --force
docker build -t stock .