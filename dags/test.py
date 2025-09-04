from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os

# ------------------------------
# Optional: function containing Spark code (not executed on import)
# ------------------------------
spark_script_path = "/opt/airflow/dags/spark_minio_job.py"

with open(spark_script_path, "w") as f:
    f.write('''
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date

spark = SparkSession.builder \\
    .appName("ProcessingTask") \\
    .master("spark://spark-master:7077") \\
    .config("spark.jars.packages", ",".join([
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.spark:spark-avro_2.12:3.5.0",
        "org.postgresql:postgresql:42.7.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ])) \\
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \\
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \\
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \\
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \\
    .getOrCreate()

df = spark.read.parquet("s3a://spark/yellow_tripdata_2024-01.parquet")

df.createOrReplaceTempView("yellow_taxi")

columns = spark.sql("DESCRIBE yellow_taxi")
columns.show()
            
result = spark.sql("""
    SELECT passenger_count, AVG(trip_distance) AS avg_distance, COUNT(*) AS num_trips
    FROM yellow_taxi
    GROUP BY passenger_count
    ORDER BY passenger_count
""")
            
result.write.jdbc(
    url="jdbc:postgresql://postgres-spark:5432/mydb",
    table="yellow_trip_summary",
    mode="overwrite",  # append instead of overwrite
    properties={
        "user": "postgres",
        "password": "mypassword",
        "driver": "org.postgresql.Driver"
    }
)

result.show()
''')

# ------------------------------
# Airflow DAG
# ------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 1),
    "retries": 1,
}

with DAG(
    dag_id="spark_minio_single_file",
    default_args=default_args,
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs = 1,
) as dag:

    run_spark_job = SparkSubmitOperator(
        task_id="spark_minio_job",
        application=spark_script_path,
        conn_id="spark_default",
        executor_memory="512m",
        total_executor_cores=1,
        verbose=True,
    )
