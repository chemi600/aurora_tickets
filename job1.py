from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# =========================
# 1. SPARK SESSION
# =========================
spark = SparkSession.builder \
    .appName("events_etl") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

base_path = "s3a://aurora-10/raw/"

# =========================
# 2. READ
# =========================
events = spark.read.csv(base_path + "events.csv", header=True)
campaigns = spark.read.csv(base_path + "campaigns.csv", header=True)
transactions = spark.read.csv(base_path + "transactions.csv", header=True)
clickstream = spark.read.json(base_path + "aurora_clickstream.jsonl")

# =========================
# 3. CAST TIPOS
# =========================
events = events \
    .withColumn("event_id", col("event_id").cast("string")) \
    .withColumn("event_date", to_date("event_date")) \
    .withColumn("base_price", col("base_price").cast("double")) \
    .withColumn("capacity", col("capacity").cast("int")) \
    .withColumn("is_active", col("is_active").cast("boolean"))

campaigns = campaigns \
    .withColumn("utm_campaign", col("utm_campaign").cast("string")) \
    .withColumn("monthly_cost", col("monthly_cost").cast("double")) \
    .withColumn("start_dt", to_date("start_dt")) \
    .withColumn("end_dt", to_date("end_dt"))

transactions = transactions \
    .withColumn("event_id", col("event_id").cast("string")) \
    .withColumn("session_id", col("session_id").cast("string")) \
    .withColumn("amount", col("amount").cast("double")) \
    .withColumn("quantity", col("quantity").cast("int")) \
    .withColumn("timestamp", to_timestamp("timestamp"))

clickstream = clickstream \
    .withColumn("event_id", col("event_id").cast("string")) \
    .withColumn("session_id", col("session_id").cast("string")) \
    .withColumn("timestamp", to_timestamp("timestamp"))

# =========================
# 4. LIMPIEZA
# =========================

# Nulos clave
events = events.dropna(subset=["event_id"])
campaigns = campaigns.dropna(subset=["utm_campaign"])
transactions = transactions.dropna(subset=["event_id", "session_id", "amount"])
clickstream = clickstream.dropna(subset=["session_id"])


clickstream = clickstream.filter(col("event_id").isNotNull())

# Outliers (IQR)
q1, q3 = transactions.approxQuantile("amount", [0.25, 0.75], 0.05)
iqr = q3 - q1

transactions = transactions.filter(
    (col("amount") >= q1 - 1.5 * iqr) &
    (col("amount") <= q3 + 1.5 * iqr)
)

# Fechas válidas
transactions = transactions.filter(
    (col("timestamp") >= lit("2020-01-01")) &
    (col("timestamp") <= current_timestamp())
)

clickstream = clickstream.filter(
    (col("timestamp") >= lit("2020-01-01")) &
    (col("timestamp") <= current_timestamp())
)

# =========================
# 5. DEDUP TRANSACTIONS
# =========================
window = Window.partitionBy("event_id", "session_id").orderBy(col("amount").desc())

transactions = transactions \
    .withColumn("rank", row_number().over(window)) \
    .filter(col("rank") == 1) \
    .drop("rank")

# =========================
# 6. ALIAS
# =========================
click = clickstream.alias("click")
evt = events.alias("evt")
camp = campaigns.alias("camp")
txn = transactions.alias("txn")

# =========================
# 7. JOINS
# =========================
df = click \
    .join(evt, col("click.event_id") == col("evt.event_id"), "inner") \
    .join(camp, col("click.utm_campaign") == col("camp.utm_campaign"), "left") \
    .join(
        txn,
        (col("click.session_id") == col("txn.session_id")) &
        (col("click.event_id") == col("txn.event_id")),
        "left"
    )

# =========================
# 8. SELECT LIMPIO 
# =========================
df = df.select(
    col("click.event_id"),
    col("click.session_id"),
    col("click.timestamp"),
    col("click.dt"),
    col("click.utm_campaign").alias("utm_campaign"),
    col("click.event_type"),
    col("click.ip"),
    col("evt.name"),
    col("evt.city"),
    col("evt.category"),

    col("camp.channel"),
    col("camp.monthly_cost"),

    col("txn.transaction_id"),
    col("txn.amount"),
    col("txn.payment_method"),
    col("txn.utm_campaign").alias("txn_utm_campaign")
)

# =========================
# 9. DATA QUALITY
# =========================
df = df.withColumn(
    "campaign_mismatch",
    when(col("utm_campaign") != col("txn_utm_campaign"), 1).otherwise(0)
)

# Unificar partición
df = df.withColumn(
    "dt",
    coalesce(col("dt"), to_date(col("timestamp")))
)

# =========================
# 10. WRITE PARQUET PARTICIONADO
# =========================
df \
  .repartition("dt") \
  .write \
  .mode("overwrite") \
  .option("compression", "snappy") \
  .partitionBy("dt") \
  .parquet("s3a://aurora-10/curated/")