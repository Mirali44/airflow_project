# 🚖 Spark + Airflow Project: example

## 1) Find Open Source Dataset
We will use the **NYC Taxi Trips**, which is publicly available:
- **Link**: wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
- Format: Parquet
- Each monthly file is ~150–200 MB

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
```

---
## 2) Write Spark Processing File
We’ll process the dataset using **both wide and narrow transformations**:

- **Narrow dependency** → `filter`, `map`, `withColumn`
- **Wide dependency** → `groupBy`, `join`

## 3) Make DAG for Spark Job (Scheduled every 10 minutes)

---
## 4) Put It on Airflow
---

## 5) Trigger DAG
From Airflow UI:
- Go to **DAGs** page.
- Enable `yourdag`.
- Click **Trigger DAG**.

## 6) Screenshots
Add screenshots here after successful run:
