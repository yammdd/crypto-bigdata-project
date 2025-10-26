### Clone the Project

Open the folder `crypto-bigdata-project`:

```bash
cd crypto-bigdata-project
```

Create a folder named `models`:

```
crypto-bigdata-project/models
```

Build and start the Docker containers:

```bash
docker compose up -d --build
```

After that, access the Spark master container:

```bash
docker exec -it spark-master bash
```

Set the Spark path:

```bash
export PATH=$PATH:/opt/spark/bin
```

Then run the following commands sequentially:

```bash
spark-submit /opt/spark/work-dir/batch/data_pipeline_1h.py
spark-submit /opt/spark/work-dir/batch/data_pipeline_1d.py
spark-submit /opt/spark/work-dir/batch/write_to_mongo.py
```

**Note:** Run these commands only once periodically (e.g., every few hours or daily) to update the model and batch layer data.

* **Stream layer:** [http://localhost:5000](http://localhost:5000)
* **Batch layer:** [http://localhost:8501](http://localhost:8501)
