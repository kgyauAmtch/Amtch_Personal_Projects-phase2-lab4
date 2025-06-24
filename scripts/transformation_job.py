import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, unix_timestamp, round, lower, upper, ltrim, rtrim, length
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Define Schemas
user_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("driver_license_number", StringType(), True),
    StructField("driver_license_expiry", StringType(), True),
    StructField("creation_date", StringType(), True),
    StructField("is_active", IntegerType(), True)
])

rental_schema = StructType([
    StructField("rental_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("rental_start_time", StringType(), True),
    StructField("rental_end_time", StringType(), True),
    StructField("pickup_location", IntegerType(), True),
    StructField("dropoff_location", IntegerType(), True),
    StructField("total_amount", StringType(), True)
])

vehicle_schema = StructType([
    StructField("active", IntegerType(), True),
    StructField("vehicle_license_number", StringType(), True),
    StructField("registration_name", StringType(), True),
    StructField("license_type", StringType(), True),
    StructField("expiration_date", StringType(), True),
    StructField("permit_license_number", StringType(), True),
    StructField("certification_date", StringType(), True),
    StructField("vehicle_year", IntegerType(), True),
    StructField("base_telephone_number", StringType(), True),
    StructField("base_address", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("last_update_timestamp", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("vehicle_type", StringType(), True)
])

location_schema = StructType([
    StructField("location_id", IntegerType(), True),
    StructField("location_name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True)
])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_source', type=str, required=True)
    parser.add_argument('--output_url', type=str, required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("DataTransformation").getOrCreate()

    # Read input data
    df_users = spark.read.option("header", True).schema(user_schema).csv(f"{args.data_source}users.csv")
    df_rentals = spark.read.option("header", True).schema(rental_schema).csv(f"{args.data_source}rental_transactions.csv")
    df_vehicles = spark.read.option("header", True).schema(vehicle_schema).csv(f"{args.data_source}vehicles.csv")
    df_locations = spark.read.option("header", True).schema(location_schema).csv(f"{args.data_source}locations.csv")

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
    df_rentals = df_rentals.withColumn("total_amount", col("total_amount").cast(DoubleType())) \
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
