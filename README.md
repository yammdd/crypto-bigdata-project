
<h1 align="center"> Crypto Big Data Platform </h1>

<p align="center">
A full end-to-end Real-Time + Batch Big Data system for cryptocurrency analytics<br>
Spark • Kafka • HBase • Airflow • HDFS • MongoDB • XGBoost • LLM Chatbot
</p>


<p align="center"> <img src="https://img.shields.io/badge/Big%20Data-Spark%2C%20Kafka%2C%20HBase-orange?style=for-the-badge"> <img src="https://img.shields.io/badge/Deployment-Docker%20Compose-blue?style=for-the-badge"> <img src="https://img.shields.io/badge/ML-XGBoost%2C%20Time%20Series-green?style=for-the-badge"> <img src="https://img.shields.io/badge/Dashboard-Streamlit%2C%20Flask-purple?style=for-the-badge"> </p>

A complete end-to-end Big Data pipeline for real-time cryptocurrency analytics.

This project demonstrates **Lambda Architecture** using:

- Kafka for ingestion

- Spark Streaming for real-time processing

- Spark Batch + Airflow for scheduled ML training

- HDFS + HBase + MongoDB for storage layers

- XGBoost for crypto price prediction

- Flask + Streamlit for UI layers (real-time & batch)

- Gemini LLM + NewsAPI for sentiment-aware market analysis

---

## 🧩 Project Setup

### 1️⃣ Clone the Project

Then open your terminal and navigate to the desired directory:

```bash
git clone https://github.com/yourusername/crypto-bigdata-project
cd crypto-bigdata-project
```

---

### 2️⃣ Build and Start Docker Containers

Run the following command to build and launch all services, **make sure you have Docker running**:

```bash
docker compose up -d --build
```

This will automatically start:
| Component                 | Purpose                                 |
|---------------------------|-------------------------------------------|
| **Kafka + Zookeeper**     | Message queue                             |
| **Spark Master + Worker** | Batch + streaming processing              |
| **HDFS (Namenode/Datanode)** | Distributed storage                  |
| **HBase**                 | Real-time serving layer                   |
| **MongoDB**               | Batch aggregation storage                 |
| **Airflow**               | Workflow scheduler                        |
| **Streamlit**             | Batch dashboard                           |
| **Flask**                 | Real-time dashboard + chatbot             |
| **Producer**              | Binance → Kafka WebSocket bridge          |


---

### 3️⃣ Setup Batch jobs

This step is how you can get XGBoost price prediction models and historical data.

After starting Docker:
```bash
docker logs airflow | findstr /i "password"
```

The result will show your Airflow `username` and `password` like:
```bash
Login with username: admin  password: nUPv6yYUp5WRRD94
```

Use this on `localhost:3636` and trigger DAG for the 1st run.

This pipeline will:

- Pull 2 years of historical data

- Store them in HDFS

- Train XGBoost models

- Write predictions to MongoDB

> 💡Tips:
> 
> Press F5 once or twice (refresh `localhost:3636`) in case no username or password is shown upon using the aforementioned command.
> 
> In file `airflow/dags/batch_pipeline_dag.py`, set `schedule_interval="@daily"` and it will update models daily when you start Docker.


---

## 📊 Dashboards

| Layer | Description | URL |
|-------|-------------|-----|
| 🟣 **Real-Time Dashboard (Flask)** | • Live price feed<br>• AI chatbot<br>• Real-time XGBoost predictions<br>• Technical indicators overlay | http://localhost:5000 |
| 🟢 **Batch Dashboard (Streamlit)** | • Historical trend analysis<br>• Model metrics (RMSE, MAPE, MAE)<br>• Risk dashboard<br>• Portfolio allocation<br>• Data explorer | http://localhost:8501 |

---
## 💻 Demo Videos:
### Batch Layer:

https://github.com/user-attachments/assets/4d2b25cb-9c87-4685-a5b5-dd1f878acbb5

### Stream Layer:

https://github.com/user-attachments/assets/7248c9d8-cfac-4a55-977d-240eaebbae8d

---

## 🧠 Used Technologies

| Category              | Tools                                      |
|-----------------------|---------------------------------------------|
| **Ingestion**         | Kafka, Binance WebSocket                    |
| **Real-time Processing** | Spark Structured Streaming                |
| **Batch Processing**  | Spark ML, XGBoost                           |
| **Scheduling**        | Airflow                                     |
| **Storage**           | HDFS, HBase, MongoDB                        |
| **Dashboards**        | Flask, Streamlit                            |
| **AI**                | Gemini LLM, NewsAPI                         |
| **Deployment**        | Docker Compose                              |

---
## 🤖 AI Chatbot Features

| Feature | Description |
|--------|-------------|
| 📰 News Summarization | Extracts and summarizes relevant crypto news articles automatically. |
| 😃 Sentiment Detection | Identifies positive/negative/neutral sentiment from live news text. |
| 📈 Technical + Fundamental Analysis | Combines market data + indicators (SMA, RSI, support/resistance) with news sentiment. |
| 🇻🇳 Vietnamese Support | Fully understands and responds in Vietnamese (and other languages). |
| 🔀 Mixed Question Handling | Can answer hybrid or complex questions and filter only the crypto-relevant parts. |
| 🧠 Short-Term Memory | Remembers the latest **10 conversation turns** for context-aware replies. |

--- 

## 👥 Contributors

<table>
  <tr>
    <td align="center">
      <a href="https://github.com/yammdd">
        <img src="https://github.com/yammdd.png" width="80" style="border-radius: 50%"><br />
        <sub><b>Thanh Dan Bui</b></sub>
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/deRuijter17">
        <img src="https://github.com/deRuijter17.png" width="80" style="border-radius: 50%"><br />
        <sub><b>Tien Dung Pham</b></sub>
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/dxpawn">
        <img src="https://github.com/dxpawn.png" width="80" style="border-radius: 50%"><br />
        <sub><b>Nguyen Dan Vu</b></sub>
      </a>
    </td>
  </tr>
</table>

---

## Notes:
> We also wrote a LaTeX report for this project. Refer to the file `bigdata_final_report.pdf`.
