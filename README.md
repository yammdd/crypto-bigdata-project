# 🚀 Crypto Big Data Project

This project builds a **complete Big Data pipeline** for analyzing cryptocurrency market data using **Spark**, **HBase**, **Kafka**, **Zookeeper**, **HDFS**, **Airflow**, **MongoDB**, **Flask** and **Streamlit**.  
It demonstrates scalable data processing, streaming updates, and interactive dashboards.

---

## 🧩 Project Setup

### 1️⃣ Clone the Project

Open your terminal and navigate to the desired directory:

```bash
cd crypto-bigdata-project
```

---

### 2️⃣ Build and Start Docker Containers

Run the following command to build and launch all services, **make sure you have Docker**:

```bash
docker compose up -d --build
```

This will automatically start:
- **Zookeeper & Kafka & Producer** (for data streaming)  
- **Spark Master & Worker nodes**  
- **Airflow & Hadoop HDFS**
- **MongoDB & HBase** (for storing processed data)  
- **Streamlit** dashboard and **Flask API**

---

### 3️⃣ Setup Batch jobs

This step is how you can get XGBoost price prediction models and historical data.

After starting Docker:
```bash
docker logs airflow | findstr "password"
```

The result will show your Airflow `username` and `password` like:
```bash
Login with username: admin  password: nUPv6yYUp5WRRD94
```

Use this on `localhost:3636` and trigger DAG for the 1st run.

> 💡Tip
> In file `airflow/dags/batch_pipeline_dag.py`, set `schedule_interval="@daily"` and it will update models daily when you start Docker.


---

## 🌐 Access Dashboards

| Layer | Description | URL |
|:------|:-------------|:----|
| **Stream Layer** | Real-time updates and streaming analytics | [http://localhost:5000](http://localhost:5000) |
| **Batch Layer** | Historical trends and model visualization | [http://localhost:8501](http://localhost:8501) |

---

## 🛠️ Useful Notes & Commands

### 🧱 **Container Management**
```bash
# List all running containers
docker ps

# View all containers (including stopped ones)
docker ps -a

# Stop all running containers
docker stop $(docker ps -q)

# Remove all stopped containers
docker rm $(docker ps -aq)

# Remove all unused images, networks, and cache
docker system prune -a
```

---

### 🧩 **Project Control**
```bash
# Rebuild and restart containers after making code changes
docker compose up -d --build

# Stop all containers for this project
docker compose down

# Stop and remove containers, networks, and volumes
docker compose down -v

# Check container logs (replace <container_name> with actual name)
docker logs <container_name> -f
```

---

### ⚙️ **Spark & Job Management**
```bash
# Access Spark master container shell
docker exec -it spark-master bash

# List Spark jobs running in cluster mode
spark-submit --status <submission_id>

# Submit a new Spark job manually
spark-submit /opt/spark/work-dir/batch/data_pipeline_1h.py

# Check Spark master web UI (on host machine)
http://localhost:8080

# Check Spark worker web UI
http://localhost:8081
```

---

### 🗃️ **MongoDB Commands**
```bash
# Access MongoDB container shell
docker exec -it mongodb bash

# Enter Mongo shell
mongosh

# Show all databases
show dbs

# Use project database
use crypto_db

# Show collections
show collections

# Query sample data
db.crypto_data.find().limit(5).pretty()
```

---

### 🧪 **Kafka Testing**
```bash
# Access Kafka container
docker exec -it kafka bash

# List Kafka topics
kafka-topics --list --bootstrap-server localhost:9092

# Describe a specific topic
kafka-topics --describe --topic crypto-stream --bootstrap-server localhost:9092

# Produce test message to a topic
kafka-console-producer --topic crypto-stream --bootstrap-server localhost:9092

# Consume messages from a topic
kafka-console-consumer --topic crypto-stream --from-beginning --bootstrap-server localhost:9092
```

---
🐱‍🐉 Congratulation you have now created a complete **end-to-end crypto analytics platform**! 💃🏻🎉

[congrat](https://github.com/user-attachments/assets/1fd4317a-af3f-477c-9449-714e778b46c7)
