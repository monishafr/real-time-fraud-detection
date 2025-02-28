# Real-Time Fraud Detection in Financial Transactions

## Overview

This repository demonstrates a **real-time pipeline** for **fraud/anomaly detection** using the **PaySim dataset**. The project showcases how to integrate:

- **Kafka** for streaming ingestion.
- **Apache Spark** for structured streaming & on-the-fly ML inference.
- **IsolationForest** (scikit-learn) as the primary unsupervised detection model.
- **Streamlit** for an interactive, real-time visualization dashboard.
- **Google Cloud Platform (GCP)** for hosting (optional if you’re not running locally).

---

## Dataset: PaySim

The **PaySim dataset** is a synthetic mobile money dataset with around 6 million rows. It is designed to mirror real financial transactions while preserving anonymity. Key features:

- **step**: Discrete time unit (could be hours).
- **type**: Transaction type (e.g., TRANSFER, CASH_OUT, PAYMENT, DEBIT).
- **amount**: Monetary value of each transaction.
- **oldbalanceOrg/newbalanceOrg**: Sender’s balance before/after the transaction.
- **oldbalanceDest/newbalanceDest**: Receiver’s balance before/after.
- **isFraud**: Indicates fraudulent (`1`) vs. legitimate (`0`).

Because it is quite large (~6.3 million rows), it’s an ideal dataset to test **streaming and big data** frameworks.

---


1. **kafka_producer.py**  
   Reads the PaySim CSV row-by-row, sending each transaction to a Kafka topic (mimics real-time).

2. **spark_streaming.py**  
   - Consumes Kafka messages via Spark Structured Streaming.
   - Loads the **IsolationForest** model to detect anomalies.
   - Writes flagged anomalies to CSV in near real time.

3. **dashboard.py**  
   - A **Streamlit** app that periodically reads the CSV of anomalies.
   - Displays charts, tables, and other insights on suspicious transactions.

4. **requirements.txt**  
   - Lists Python libraries needed (e.g., `streamlit`, `pandas`, `numpy`, `scikit-learn`, etc.).

---

## Getting Started

### 1. Clone the Repo

```bash
git clone https://github.com/YOUR_USERNAME/real-time-fraud-detection.git
cd real-time-fraud-detection
```

### 2. Install Dependencies

Make sure you have Python 3.7+ (or 3.11, etc.). Then install the required Python packages:

```bash
pip install -r requirements.txt
```

### 3. Start Kafka & Spark
- Kafka: Start ZooKeeper and Kafka server (kafka-server-start.sh, zookeeper-server-start.sh).
- Spark: Ensure spark-submit is in your PATH or specify full path, e.g. ~/spark/bin/spark-submit.

### 4. Run Kafka Producer
```bash
python kafka_producer.py
```
This script reads the Transactions.csv (path may be in the script) and sends each row to a Kafka topic (e.g., paysim_transactions).


### 5. Run Spark Streaming

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_streaming.py
```
- The script consumes from the paysim_transactions topic.
- Uses the pre-trained IsolationForest model to detect anomalies.
- Writes flagged transactions to a CSV folder in near real time.

### 6. Launch the Streamlit Dashboard

```bash
streamlit run dashboard.py
```

- By default, it’s at http://localhost:8501.
- The dashboard reads the CSVs of flagged anomalies, updating charts and tables in near-real time.

## Key Technologies

- PaySim dataset (Kaggle)
- Python 3.7+
- Kafka (Producer & Topic)
- Spark (Structured Streaming & ML inference in micro-batches)
- IsolationForest (scikit-learn)
- Local Outlier Factor
- Autoencoder (Neural Network)
- Streamlit (real-time dashboard)
- Google Cloud 


