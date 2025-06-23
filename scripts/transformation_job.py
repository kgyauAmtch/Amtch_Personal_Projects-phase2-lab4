import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, to_timestamp, to_date, year, current_date,
    when, regexp_extract, upper, lower, ltrim, rtrim, length, isnull, isnan,
    regexp_replace, unix_timestamp, round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DateType,
    TimestampType, DoubleType, BooleanType
)

# --- Define Schemas ---
# User Data Schema
user_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("driver_license_number", StringType(), True),
    StructField("driver_license_expiry", StringType(), True), # Read as String for initial validation
    StructField("creation_date", StringType(), True), # Read as String for initial validation
    StructField("is_active", IntegerType(), True) # Assuming 0 or 1
])

# Rental Transactions Data Schema
rental_schema = StructType([
    StructField("rental_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("rental_start_time", StringType(), True), # Read as String for initial validation
    StructField("rental_end_time", StringType(), True),   # Read as String for initial validation
    StructField("pickup_location", IntegerType(), True),
    StructField("dropoff_location", IntegerType(), True),
    StructField("total_amount", StringType(), True) # Read as String to handle potential non-numeric values during transformation
])

# Vehicle Data Schema
vehicle_schema = StructType([
    StructField("active", IntegerType(), True),
    StructField("vehicle_license_number", StringType(), True),
    StructField("registration_name", StringType(), True),
    StructField("license_type", StringType(), True),
    StructField("expiration_date", StringType(), True), # Read as String for initial validation
    StructField("permit_license_number", StringType(), True),
    StructField("certification_date", StringType(), True), # Read as String for initial validation
    StructField("vehicle_year", IntegerType(), True),
    StructField("base_telephone_number", StringType(), True),
    StructField("base_address", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("last_update_timestamp", StringType(), True), # Read as String for initial validation
    StructField("brand", StringType(), True),
    StructField("vehicle_type", StringType(), True)
])

# Location Data Schema
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

# --- Main execution block for command-line arguments ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark Job 1: Data Ingestion and Transformation.")
    parser.add_argument('--data_source', type=str, required=True,
                        help='Base URL for raw input data (e.g., s3://raw-bucket/raw_data/')
    parser.add_argument('--output_url', type=str, required=True,
                        help='Base URL for transformed output data (e.g., s3://your-curated-bucket/transformed_data/)')
    args = parser.parse_args()

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Job1_DataIngestionAndTransformation") \
        .getOrCreate()

    # --- File Paths (Constructed from arguments) ---
    # Input CSVs
    raw_vehicle_data_path = f"{args.data_source}vehicles.csv"
    raw_location_data_path = f"{args.data_source}locations.csv"
    raw_user_data_path = f"{args.data_source}users.csv"
    raw_rental_data_path = f"{args.data_source}rental_transactions.csv"

    # Output Parquet (Curated Zone)
    transformed_vehicles_path = f"{args.output_url}vehicles/"
    transformed_locations_path = f"{args.output_url}locations/"
    transformed_users_path = f"{args.output_url}users/"
    transformed_rentals_path = f"{args.output_url}rentals/"

    print("--- Starting Job 1: All Data Transformation and  Data Storage ---")

    # --- 2. Read Data ---
    print(f"Reading user data from {raw_user_data_path}...")
    try:
        df_users = spark.read \
            .option("header", "true") \
            .schema(user_schema) \
            .csv(raw_user_data_path)
        print("User data schema after initial read:")
        df_users.printSchema()
        df_users.show(5, truncate=False)
        df_users.cache() # Cache after reading
    except Exception as e:
        print(f"Error reading user data from {raw_user_data_path}: {e}")
        df_users = None

    print(f"\nReading rental transaction data from {raw_rental_data_path}...")
    try:
        df_rentals = spark.read \
            .option("header", "true") \
            .schema(rental_schema) \
            .csv(raw_rental_data_path)
        print("Rental data schema after initial read:")
        df_rentals.printSchema()
        df_rentals.show(5, truncate=False)
        df_rentals.cache() # Cache after reading
    except Exception as e:
        print(f"Error reading rental data from {raw_rental_data_path}: {e}")
        df_rentals = None

    print(f"\nReading vehicle data from {raw_vehicle_data_path}...")
    try:
        df_vehicles = spark.read \
            .option("header", "true") \
            .schema(vehicle_schema) \
            .csv(raw_vehicle_data_path)
        print("Vehicle data schema after initial read:")
        df_vehicles.printSchema()
        df_vehicles.show(5, truncate=False)
        df_vehicles.cache() # Cache after reading
    except Exception as e:
        print(f"Error reading vehicle data from {raw_vehicle_data_path}: {e}")
        df_vehicles = None

    print(f"\nReading location data from {raw_location_data_path}...")
    try:
        df_locations = spark.read \
            .option("header", "true") \
            .schema(location_schema) \
            .csv(raw_location_data_path)
        print("Location data schema after initial read:")
        df_locations.printSchema()
        df_locations.show(5, truncate=False)
        df_locations.cache() # Cache after reading
    except Exception as e:
        print(f"Error reading location data from {raw_location_data_path}: {e}")
        df_locations = None


    # --- Basic Validations and Transformations (Vehicle Data) ---
    if df_vehicles:
        print("\n--- Validating and Transforming Vehicle Data ---")

        #Null Checks and Handling
        print("\nChecking for nulls in critical vehicle columns...")
        critical_vehicle_cols = ["vehicle_license_number", "expiration_date", "vehicle_id", "brand", "vehicle_type"]
        for col_name in critical_vehicle_cols:
            null_count = df_vehicles.filter(col(col_name).isNull()).count()
            if null_count > 0:
                print(f"WARNING: Column '{col_name}' has {null_count} null values. Dropping rows.")
                df_vehicles = df_vehicles.na.drop(subset=[col_name])
                print(f"Remaining rows after dropping nulls in '{col_name}': {df_vehicles.count()}")

        # Data Type Conversions and Formatting
        print("\nPerforming data type conversions and formatting for vehicle data...")

        df_vehicles_transformed = df_vehicles.withColumn("expiration_date", to_date(col("expiration_date"), "dd-MM-yyyy")) \
                                             .withColumn("certification_date", to_date(col("certification_date"), "yyyy-MM-dd")) \
                                             .withColumn("last_update_timestamp", to_timestamp(col("last_update_timestamp"), "dd-MM-yyyy HH:mm:ss"))

        # Validate date conversions (check for nulls after conversion, indicating parse failures)
        initial_rows = df_vehicles_transformed.count()
        df_vehicles_transformed = df_vehicles_transformed.filter(col("expiration_date").isNotNull()) \
                                                         .filter(col("certification_date").isNotNull()) \
                                                         .filter(col("last_update_timestamp").isNotNull())
        # invalid_date_rows = initial_rows - df_vehicles_transformed.count()
        # if invalid_date_rows > 0:
        #     print(f"WARNING: Dropped {invalid_date_rows} rows due to invalid date formats in expiration_date, certification_date, or last_update_timestamp.")

        # Validate 'active' column: should be 0 or 1
        initial_rows = df_vehicles_transformed.count()
        df_vehicles_transformed = df_vehicles_transformed.filter(col("active").isin(0, 1))
        invalid_active_rows = initial_rows - df_vehicles_transformed.count()
        # if invalid_active_rows > 0:
        #     print(f"WARNING: Dropped {invalid_active_rows} rows with invalid 'active' values (not 0 or 1).")

        # Trim whitespace from string columns
        string_cols_to_trim = ["registration_name", "license_type", "permit_license_number", "base_address", "brand", "vehicle_type"]
        for col_name in string_cols_to_trim:
            df_vehicles_transformed = df_vehicles_transformed.withColumn(col_name, ltrim(rtrim(col(col_name))))

    

        print("\nSchema after vehicle transformations:")
        df_vehicles_transformed.printSchema()
        print("\nSample Vehicle Data after transformations:")
        df_vehicles_transformed.select("active", "vehicle_license_number", "expiration_date", "certification_date", "vehicle_year", "base_address", "license_type", "brand", "vehicle_type").show(5, truncate=False)

        # Write transformed vehicle data to Parquet
        print(f"Saving transformed vehicle data to {transformed_vehicles_path}")
        df_vehicles_transformed.write.mode("overwrite").parquet(transformed_vehicles_path)
        df_vehicles_transformed.unpersist() # Free memory after saving


    # --- Basic Validations and Transformations (User Data) ---
    if df_users:
        print("\n--- Validating and Transforming User Data ---")

        # Null Checks and Handling
        print("\nChecking for nulls in critical user columns...")
        critical_user_cols = ["user_id", "email", "phone_number", "creation_date", "is_active"]
        for col_name in critical_user_cols:
            null_count = df_users.filter(col(col_name).isNull()).count()
            if null_count > 0:
                print(f"WARNING: Column '{col_name}' has {null_count} null values. Dropping rows.")
                df_users = df_users.na.drop(subset=[col_name])
                print(f"Remaining rows after dropping nulls in '{col_name}': {df_users.count()}")

        # Data Type Conversions and Formatting
        print("\nPerforming data type conversions and formatting for user data...")

        # Convert date columns
        df_users_transformed = df_users.withColumn("driver_license_expiry", to_date(col("driver_license_expiry"), "yyyy-MM-dd")) \
                                       .withColumn("creation_date", to_date(col("creation_date"), "yyyy-MM-dd"))

        
        # Phone number validation (simple regex for common formats including extensions)
        phone_regex = r"^(\+?\d{1,3}[-. ]?)?\(?\d{3}\)?[-. ]?\d{3}[-. ]?\d{4}(x\d+)?$"
        initial_rows = df_users_transformed.count()
        df_users_transformed = df_users_transformed.filter(col("phone_number").rlike(phone_regex))
        invalid_phone_numbers = initial_rows - df_users_transformed.count()
        if invalid_phone_numbers > 0:
            print(f"WARNING: Dropped {invalid_phone_numbers} rows due to invalid 'phone_number' format.")

        # Trim whitespace from string columns
        trim_cols = ["first_name", "last_name", "email", "phone_number", "driver_license_number"]
        for col_name in trim_cols:
            df_users_transformed = df_users_transformed.withColumn(col_name, ltrim(rtrim(col(col_name))))

        # Standardize first_name and last_name to all lower
        df_users_transformed = df_users_transformed.withColumn("first_name", lower(col("first_name"))) \
                                                    .withColumn("last_name", lower(col("last_name")))

        print("\nSchema after user transformations:")
        df_users_transformed.printSchema()
        print("\nSample User Data after transformations:")
        df_users_transformed.select("user_id", "first_name", "last_name", "email", "phone_number", "driver_license_expiry", "creation_date", "is_active").show(5, truncate=False)

        # Write transformed user data to Parquet
        print(f"Saving transformed user data to {transformed_users_path}")
        df_users_transformed.write.mode("overwrite").parquet(transformed_users_path)
        df_users_transformed.unpersist() # Free memory after saving


    # --- Basic Validations and Transformations (Rental Transaction Data) ---
    if df_rentals:
        print("\n--- Validating and Transforming Rental Transaction Data ---")

        # Null Checks and Handling
        print("\nChecking for nulls in critical rental columns...")
        critical_rental_cols = ["rental_id", "user_id", "vehicle_id", "rental_start_time", "rental_end_time", "total_amount"]
        for col_name in critical_rental_cols:
            null_count = df_rentals.filter(col(col_name).isNull()).count()
            if null_count > 0:
                print(f"WARNING: Column '{col_name}' has {null_count} null values. Dropping rows.")
                df_rentals = df_rentals.na.drop(subset=[col_name])
                print(f"Remaining rows after dropping nulls in '{col_name}': {df_rentals.count()}")

        # Data Type Conversions and Formatting
        print("\nPerforming data type conversions and formatting for rental data...")

        # Cast total_amount to Double, handling potential errors gracefully (coalesce to null if bad)
        df_rentals_transformed = df_rentals.withColumn("total_amount", col("total_amount").cast(DoubleType())) \
                                           .withColumn("rental_start_time", to_timestamp(col("rental_start_time"), "yyyy-MM-dd HH:mm:ss")) \
                                           .withColumn("rental_end_time", to_timestamp(col("rental_end_time"), "yyyy-MM-dd HH:mm:ss"))

        # Filter out rows where conversions failed or total_amount is invalid/negative
        initial_rows = df_rentals_transformed.count()
        df_rentals_transformed = df_rentals_transformed.filter(col("total_amount").isNotNull()) \
                                                       .filter(col("total_amount") >= 0) \
                                                       .filter(col("rental_start_time").isNotNull()) \
                                                       .filter(col("rental_end_time").isNotNull()) \
                                                       .filter(col("rental_start_time") < col("rental_end_time"))
        invalid_rental_rows = initial_rows - df_rentals_transformed.count()
        if invalid_rental_rows > 0:
            print(f"WARNING: Dropped {invalid_rental_rows} rows due to invalid total_amount, dates, or rental duration issues.")

        # Add common derived columns for analytics
        df_rentals_transformed = df_rentals_transformed.withColumn("rental_duration_seconds",
                                                       unix_timestamp(col("rental_end_time")) - unix_timestamp(col("rental_start_time"))) \
                                                       .withColumn("rental_duration_hours",
                                                       round((col("rental_duration_seconds") / 3600), 2)) \
                                                       .withColumn("rental_date", to_date(col("rental_start_time")))


        print("\nSchema after rental transformations:")
        df_rentals_transformed.printSchema()
        print("\nSample Rental Transaction Data after transformations:")
        df_rentals_transformed.select("rental_id", "user_id", "vehicle_id", "rental_start_time", "rental_end_time", "total_amount", "rental_duration_hours", "rental_date").show(5, truncate=False)

        # Write transformed rental data to Parquet
        print(f"Saving transformed rental data to {transformed_rentals_path}")
        df_rentals_transformed.write.mode("overwrite").parquet(transformed_rentals_path)
        df_rentals_transformed.unpersist() # Free memory after saving


    # --- Basic Validations and Transformations (Location Data) ---
    if df_locations:
        print("\n--- Validating and Transforming Location Data ---")

        # Null Checks and Handling
        print("\nChecking for nulls in critical location columns...")
        critical_location_cols = ["location_id", "location_name", "address", "city", "state", "zip_code", "latitude", "longitude"]
        for col_name in critical_location_cols:
            null_count = df_locations.filter(col(col_name).isNull()).count()
            if null_count > 0:
                print(f"WARNING: Column '{col_name}' has {null_count} null values. Dropping rows.")
                df_locations = df_locations.na.drop(subset=[col_name])
                print(f"Remaining rows after dropping nulls in '{col_name}': {df_locations.count()}")

        # Data Type Conversions and Formatting
        print("\nPerforming data type conversions and formatting for location data...")

        # Ensure latitude and longitude are within valid ranges
        initial_rows = df_locations.count()
        df_locations_transformed = df_locations.filter((col("latitude") >= -90) & (col("latitude") <= 90)) \
                                               .filter((col("longitude") >= -180) & (col("longitude") <= 180))
        invalid_geo_rows = initial_rows - df_locations_transformed.count()
        if invalid_geo_rows > 0:
            print(f"WARNING: Dropped {invalid_geo_rows} rows due to invalid 'latitude' or 'longitude' values (out of range).")

        # Trim whitespace from string columns
        trim_cols = ["location_name", "address", "city", "state", "zip_code"]
        for col_name in trim_cols:
            df_locations_transformed = df_locations_transformed.withColumn(col_name, ltrim(rtrim(col(col_name))))

        # Standardize 'state' to uppercase
        df_locations_transformed = df_locations_transformed.withColumn("state", upper(col("state")))

        # Validate zip_code length (assuming 5 digits)
        initial_rows = df_locations_transformed.count()
        df_locations_transformed = df_locations_transformed.filter(length(col("zip_code")) == 5)
        invalid_zip_rows = initial_rows - df_locations_transformed.count()
        if invalid_zip_rows > 0:
            print(f"WARNING: Dropped {invalid_zip_rows} rows due to invalid 'zip_code' values (not 5 digits).")


        print("\nSchema after location transformations:")
        df_locations_transformed.printSchema()
        print("\nSample Location Data after transformations:")
        df_locations_transformed.select("location_id", "location_name", "address", "city", "state", "zip_code", "latitude", "longitude").show(5, truncate=False)

        # Write transformed location data to Parquet
        print(f"Saving transformed location data to {transformed_locations_path}")
        df_locations_transformed.write.mode("overwrite").parquet(transformed_locations_path)
        df_locations_transformed.unpersist() # Free memory after saving

    # Stop Spark Session
    spark.stop()
    print("\nSpark Session stopped. All transformed data saved to Parquet folders.")