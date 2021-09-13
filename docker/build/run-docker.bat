docker stop stock
docker rm stock
docker run -itd --rm --network presto-network --name stock stock