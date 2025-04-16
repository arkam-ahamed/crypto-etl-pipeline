# 💸 Crypto ETL Pipeline

A real-time Bitcoin transaction generator pipeline built using **Apache Kafka**, **Apache Spark**, and **Elasticsearch**. It simulates dummy Bitcoin transaction data, cleans it with Apache Spark, and stores valid transactions in Elasticsearch and visualizes using Kibana

## 🚀 Architecture

Kafka Producer (Spring Boot) → Kafka Topic → Spark Structured Streaming → Elasticsearch -> Kibana

## 📦 Spin It Up (Easy Setup)

Make sure you have **Docker** and **Docker Compose** installed.

```bash
git clone https://github.com/arkam-ahamed/crypto-etl-pipeline.git
cd crypto-etl-pipeline
docker-compose up --build
