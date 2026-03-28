from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("events_etl") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
df = spark.read.parquet("s3a://aurora-10/curated/")

# Consulta 1
funnel = df.groupBy("dt", "session_id").agg(
    max(when(col("event_type") == "page_view", 1).otherwise(0)).alias("view"),
    max(when(col("event_type") == "event_list", 1).otherwise(0)).alias("list"),
    max(when(col("event_type") == "event_detail", 1).otherwise(0)).alias("detail"),
    max(when(col("event_type") == "begin_checkout", 1).otherwise(0)).alias("checkout"),
    max(when(col("transaction_id").isNotNull(), 1).otherwise(0)).alias("purchase")
)

funnel_daily = funnel.groupBy("dt").agg(
    countDistinct("session_id").alias("sessions_total"),
    sum("list").alias("sessions_event_list"),
    sum("detail").alias("sessions_event_detail"),
    sum("checkout").alias("sessions_begin_checkout"),
    sum("purchase").alias("sessions_purchase")
).withColumn(
    "conversion_rate",
    col("sessions_purchase") / col("sessions_total")
)

funnel_daily.write.mode("overwrite").parquet("s3a://aurora-10/analytics/funnel/")

# Consulta 2
events_rank = df.groupBy("dt", "event_id").agg(
    sum(when(col("event_type") == "event_detail", 1).otherwise(0)).alias("detail_views"),
    sum(when(col("transaction_id").isNotNull(), 1).otherwise(0)).alias("purchases"),
    sum("amount").alias("revenue_total")
).withColumn(
    "interest_to_purchase_ratio",
    col("purchases") / col("detail_views")
)

events_rank.write.mode("overwrite").parquet("s3a://aurora-10/analytics/events_rank/")


# Consulta 3

ip_stats = df.groupBy("dt", "ip").agg(
    count("*").alias("requests"),
    sum(when(col("transaction_id").isNotNull(), 1).otherwise(0)).alias("purchases")
)

anomalies = ip_stats.withColumn(
    "is_anomaly",
    when((col("requests") > 100) & (col("purchases") == 0), 1).otherwise(0)
).withColumn(
    "reason",
    when(col("is_anomaly") == 1, "High traffic without purchases")
)

anomalies.write.mode("overwrite").parquet("s3a://aurora-10/analytics/anomalies/")

import boto3
import json
from botocore.exceptions import ClientError


def get_secret():

    secret_name = "aurora/password"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']
    return secret
sec = json.loads(get_secret())
mysql_url = f"jdbc:mysql://{sec.get('host')}:{sec.get('port')}/aurora"
mysql_properties = {
    "user": sec.get('username'),
    "password": sec.get('password'),
    "driver": "com.mysql.cj.jdbc.Driver"
}

funnel_daily.write.jdbc(url=mysql_url, table="metrics_funnel_daily", mode="overwrite", properties=mysql_properties)
events_rank.write.jdbc(url=mysql_url, table="metrics_event_rank", mode="overwrite", properties=mysql_properties)
anomalies.write.jdbc(url=mysql_url, table="metrics_anomalies", mode="overwrite", properties=mysql_properties)

print("Inserción completada con éxito")