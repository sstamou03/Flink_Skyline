@echo off
docker-compose down -v
docker-compose up -d --scale taskmanager=4


docker exec kafka kafka-topics --create --topic Input --bootstrap-server kafka:29092 --partitions 4 --replication-factor 1
docker exec kafka kafka-topics --create --topic Input2 --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --create --topic Output --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1

docker cp "C:\Users\stamo\Desktop\FlinkSkyline\out\artifacts\FlinkSkyline_jar\FlinkSkyline.jar" jobmanager:/job.jar


pause