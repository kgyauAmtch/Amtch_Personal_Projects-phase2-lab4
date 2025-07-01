import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, to_timestamp, unix_timestamp, round,
    lower, ltrim, rtrim, sum, count, avg, max, min, lit
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_source', type=str, required=True, help="S3 path for input CSVs (with trailing slash)")
    parser.add_argument('--output_url', type=str, required=True, help="S3 path for output Parquet data (with trailing slash)")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("UserTransactionMetrics") \
        .config("spark.hadoop.fs.s3a.create.folder.marker", "false") \
        .getOrCreate()

    # Read CSVs with header and infer schema
    df_users = spark.read.option("header", True).option("inferSchema", True).csv(f"{args.data_source}users.csv")
    df_rentals = spark.read.option("header", True).option("inferSchema", True).csv(f"{args.data_source}rental_transactions.csv")

    # --- Transform User Data ---
    df_users = df_users.dropna(subset=["user_id", "email", "phone_number", "creation_date", "is_active"])
    df_users = df_users.withColumn("driver_license_expiry", to_date("driver_license_expiry", "yyyy-MM-dd")) \
                       .withColumn("creation_date", to_date("creation_date", "yyyy-MM-dd"))

    for col_name in ["first_name", "last_name", "email", "phone_number", "driver_license_number"]:
        df_users = df_users.withColumn(col_name, ltrim(rtrim(col(col_name))))

    df_users = df_users.withColumn("first_name", lower("first_name")) \
                       .withColumn("last_name", lower("last_name"))

    df_users.write.mode("overwrite").parquet(f"{args.output_url}users/")

    # --- Transform Rental Data (needed for metrics) ---
    df_rentals = df_rentals.dropna(subset=["rental_id", "user_id", "vehicle_id", "rental_start_time", "rental_end_time", "total_amount"])
    df_rentals = df_rentals.withColumn("total_amount", col("total_amount").cast("double")) \
                           .withColumn("rental_start_time", to_timestamp("rental_start_time", "yyyy-MM-dd HH:mm:ss")) \
                           .withColumn("rental_end_time", to_timestamp("rental_end_time", "yyyy-MM-dd HH:mm:ss")) \
                           .filter((col("total_amount") >= 0) & (col("rental_start_time") < col("rental_end_time")))

    df_rentals = df_rentals.withColumn("rental_duration_seconds", unix_timestamp("rental_end_time") - unix_timestamp("rental_start_time")) \
                           .withColumn("rental_duration_hours", round(col("rental_duration_seconds") / 3600, 2)) \
                           .withColumn("rental_date", to_date("rental_start_time"))

    # ───────────────────── Transaction Metrics ─────────────────────
    daily_metrics = df_rentals.groupBy("rental_date").agg(
        count("rental_id").alias("total_transactions"),
        round(sum("total_amount"), 2).alias("total_revenue"),
        round(avg("total_amount"), 2).alias("average_transaction"),
        round(max("total_amount"), 2).alias("max_transaction"),
        round(min("total_amount"), 2).alias("min_transaction")
    )

    overall_metrics = df_rentals.agg(
        round(max("total_amount"), 2).alias("overall_max_transaction"),
        round(min("total_amount"), 2).alias("overall_min_transaction")
    ).withColumn("metric_type", lit("overall"))

    daily_metrics.write.mode("overwrite").parquet(f"{args.output_url}transaction_metrics/daily/")
    overall_metrics.write.mode("overwrite").parquet(f"{args.output_url}transaction_metrics/overall/")

    # ───────────────────── User Metrics ─────────────────────
    df_rentals_users = df_rentals.alias("r").join(
        df_users.alias("u"),
        col("r.user_id") == col("u.user_id"),
        "inner"
    ).select("r.*", "u.first_name", "u.last_name", "u.email")

    user_metrics = df_rentals_users.groupBy("user_id", "first_name", "last_name", "email").agg(
        count("rental_id").alias("total_transactions"),
        round(sum("total_amount"), 2).alias("total_spending"),
        round(max("total_amount"), 2).alias("max_spending"),
        round(min("total_amount"), 2).alias("min_spending"),
        round(sum("rental_duration_hours"), 2).alias("total_rental_hours")
    )

    user_metrics.write.mode("overwrite").parquet(f"{args.output_url}user_metrics/")

    spark.stop()