# Distributed Skyline Query Processing with Apache Flink

## Team Members
* **Spyridon Stamou** (AM: 2021030090) 
* **Georgios Angelos Gravalos** (AM: 2021030001) 

## Project Description
This project implements and evaluates distributed Skyline (Dominating) query algorithms using **Apache Flink** and **Apache Kafka**. Specifically, we implemented and compared three MapReduce-based partitioning strategies: **MR-Dim**, **MR-Grid**, and **MR-Angle**. The system is designed to process large-scale data streams ,up to 15 million tuples, and assesses the performance and scalability of the algorithms across different data distributions (Uniform, Correlated, Anti-correlated).

## Prerequisites
* **Docker** & **Docker Compose**
* **Java 11** (JDK)
* **SBT** (Scala Build Tool)

## Execution Flow

### Step 1: Initialize Environment
Run the `setup_env.bat` script. This script performs the following actions:
1.  **Resets Docker:** Tears down any running containers (`docker-compose down -v`) to ensure a clean state.
2.  **Scales Cluster:** Starts the cluster with **12 TaskManagers** (Workers).
3.  **Creates Topics:** Automatically creates the Kafka topics:
    * `Input` (4 partitions)
    * `Input2` (1 partition - Trigger)
    * `Output` (1 partition - Results)
4.  **Deploys JAR:** Copies the compiled JAR file from the host machine to the `jobmanager` container.

### Step 2: Submit the Job
```batch
docker exec -it jobmanager ./bin/flink run -c ECE613.Job /job.jar \
  --parallelism 4 \
  --partitions 8 \
  --bootstrap.servers kafka:29092 \
  --topic Input \
  --request_topic Input2 \
  --output_topic Output \
  --MR angle \
  --vmax 1000.0
```
### Step 3: Kafka Terminals

You need two separate terminals to interact with the system:

Terminal A :
```batch 
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic Output
```

Terminal B : 
```batch
docker exec -it kafka kafka-console-producer --bootstrap-server kafka:29092 --topic Input2
```
### Step 4: Data Generation

Finally, run the Generator.scala to start feeding data into the Input topic.

