import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, sum, count, avg, max, min, countDistinct, round
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark Job 2: KPI Calculation.")
    parser.add_argument('--data_source', type=str, required=True, help='S3 path for transformed data')
    parser.add_argument('--output_url', type=str, required=True, help='S3 path for output metrics')
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("KPI_Calculation") \
        .config("spark.hadoop.fs.s3a.create.folder.marker", "false") \
        .getOrCreate()

    # Input paths
    df_vehicles = spark.read.parquet(f"{args.data_source}vehicles/").cache()
    df_locations = spark.read.parquet(f"{args.data_source}locations/").cache()
    df_users = spark.read.parquet(f"{args.data_source}users/").cache()
    df_rentals = spark.read.parquet(f"{args.data_source}rentals/").cache()

    # ───────────────────── Location Metrics ─────────────────────
    df_rentals_loc = df_rentals.alias("r").join(
        df_locations.alias("l"),
        col("r.pickup_location") == col("l.location_id"),
        "inner"
    ).select("r.*", "l.location_id", "l.location_name", "l.city", "l.state")

    location_metrics = df_rentals_loc.groupBy("location_id", "location_name", "city", "state").agg(
        round(sum("total_amount"), 2).alias("total_revenue"),
        count("rental_id").alias("total_transactions"),
        round(avg("total_amount"), 2).alias("average_transaction"),
        round(max("total_amount"), 2).alias("max_transaction"),
        round(min("total_amount"), 2).alias("min_transaction"),
        countDistinct("vehicle_id").alias("unique_vehicles")
    )

    location_metrics.write.mode("overwrite").parquet(f"{args.output_url}location_metrics/")

    # ───────────────────── Vehicle Metrics ─────────────────────
    df_rentals_veh = df_rentals.alias("r").join(
        df_vehicles.alias("v"),
        col("r.vehicle_id") == col("v.vehicle_id"),
        "inner"
    ).select("r.*", "v.vehicle_type", "v.brand")

    vehicle_metrics = df_rentals_veh.groupBy("vehicle_type", "brand").agg(
        round(sum("total_amount"), 2).alias("total_revenue"),
        count("rental_id").alias("total_rentals"),
        round(avg("rental_duration_hours"), 2).alias("avg_rental_duration")
    )

    vehicle_metrics.write.mode("overwrite").parquet(f"{args.output_url}vehicle_metrics/")

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
    ).select("r.*", "u.user_id", "u.first_name", "u.last_name", "u.email")

    user_metrics = df_rentals_users.groupBy("user_id", "first_name", "last_name", "email").agg(
        count("rental_id").alias("total_transactions"),
        round(sum("total_amount"), 2).alias("total_spending"),
        round(max("total_amount"), 2).alias("max_spending"),
        round(min("total_amount"), 2).alias("min_spending"),
        round(sum("rental_duration_hours"), 2).alias("total_rental_hours")
    )

    user_metrics.write.mode("overwrite").parquet(f"{args.output_url}user_metrics/")

    # ───────────────────── Cleanup ─────────────────────
    df_vehicles.unpersist()
    df_locations.unpersist()
    df_users.unpersist()
    df_rentals.unpersist()
    df_rentals_loc.unpersist()
    df_rentals_veh.unpersist()
    df_rentals_users.unpersist()

    spark.stop()
    print("\n KPI Calculation Complete. Metrics saved in 4 folders:")
    print("• location_metrics/")
    print("• vehicle_metrics/")
    print("• transaction_metrics/ (daily/, overall/)")
    print("• user_metrics/")
