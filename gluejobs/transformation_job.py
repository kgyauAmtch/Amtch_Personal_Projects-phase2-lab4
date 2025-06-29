import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, to_timestamp, unix_timestamp, round,
    lower, upper, ltrim, rtrim, length
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_source', type=str, required=True, help="S3 path for input CSVs (with trailing slash)")
    parser.add_argument('--output_url', type=str, required=True, help="S3 path for output Parquet data (with trailing slash)")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("DataTransformation").getOrCreate()

    # Read CSVs with header and infer schema
    df_users = spark.read.option("header", True).option("inferSchema", True).csv(f"{args.data_source}users.csv")
    df_rentals = spark.read.option("header", True).option("inferSchema", True).csv(f"{args.data_source}rental_transactions.csv")
    df_vehicles = spark.read.option("header", True).option("inferSchema", True).csv(f"{args.data_source}vehicles.csv")
    df_locations = spark.read.option("header", True).option("inferSchema", True).csv(f"{args.data_source}locations.csv")

    # --- Transform Vehicle Data ---
    df_vehicles = df_vehicles.dropna(subset=["vehicle_license_number", "expiration_date", "vehicle_id", "brand", "vehicle_type"])
    df_vehicles = df_vehicles.withColumn("expiration_date", to_date("expiration_date", "dd-MM-yyyy")) \
                             .withColumn("certification_date", to_date("certification_date", "yyyy-MM-dd")) \
                             .withColumn("last_update_timestamp", to_timestamp("last_update_timestamp", "dd-MM-yyyy HH:mm:ss")) \
                             .filter(col("active").isin(0, 1))

    for col_name in ["registration_name", "license_type", "permit_license_number", "base_address", "brand", "vehicle_type"]:
        df_vehicles = df_vehicles.withColumn(col_name, ltrim(rtrim(col(col_name))))

    df_vehicles.write.mode("overwrite").parquet(f"{args.output_url}vehicles/")

    # --- Transform User Data ---
    df_users = df_users.dropna(subset=["user_id", "email", "phone_number", "creation_date", "is_active"])
    df_users = df_users.withColumn("driver_license_expiry", to_date("driver_license_expiry", "yyyy-MM-dd")) \
                       .withColumn("creation_date", to_date("creation_date", "yyyy-MM-dd"))

    for col_name in ["first_name", "last_name", "email", "phone_number", "driver_license_number"]:
        df_users = df_users.withColumn(col_name, ltrim(rtrim(col(col_name))))

    df_users = df_users.withColumn("first_name", lower("first_name")) \
                       .withColumn("last_name", lower("last_name"))

    df_users.write.mode("overwrite").parquet(f"{args.output_url}users/")

    # --- Transform Rental Data ---
    df_rentals = df_rentals.dropna(subset=["rental_id", "user_id", "vehicle_id", "rental_start_time", "rental_end_time", "total_amount"])
    df_rentals = df_rentals.withColumn("total_amount", col("total_amount").cast("double")) \
                           .withColumn("rental_start_time", to_timestamp("rental_start_time", "yyyy-MM-dd HH:mm:ss")) \
                           .withColumn("rental_end_time", to_timestamp("rental_end_time", "yyyy-MM-dd HH:mm:ss")) \
                           .filter((col("total_amount") >= 0) & (col("rental_start_time") < col("rental_end_time")))

    df_rentals = df_rentals.withColumn("rental_duration_seconds", unix_timestamp("rental_end_time") - unix_timestamp("rental_start_time")) \
                           .withColumn("rental_duration_hours", round(col("rental_duration_seconds") / 3600, 2)) \
                           .withColumn("rental_date", to_date("rental_start_time"))

    df_rentals.write.mode("overwrite").parquet(f"{args.output_url}rentals/")

    # --- Transform Location Data ---
    df_locations = df_locations.dropna(subset=["location_id", "location_name", "address", "city", "state", "zip_code", "latitude", "longitude"])
    df_locations = df_locations.filter((col("latitude") >= -90) & (col("latitude") <= 90) & (col("longitude") >= -180) & (col("longitude") <= 180))

    for col_name in ["location_name", "address", "city", "state", "zip_code"]:
        df_locations = df_locations.withColumn(col_name, ltrim(rtrim(col(col_name))))

    df_locations = df_locations.withColumn("state", upper("state")) \
                               .filter(length("zip_code") == 5)

    df_locations.write.mode("overwrite").parquet(f"{args.output_url}locations/")

    spark.stop()
