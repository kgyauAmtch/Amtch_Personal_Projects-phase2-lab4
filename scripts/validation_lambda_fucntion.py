import boto3
import csv
import io
import logging

# Enable logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# S3 Configuration
BUCKET = "lab4-bucket-gtp"
PREFIX = "raw/"
REQUIRED_FILES = {
    "users.csv": {"user_id","first_name","last_name","email","phone_number","driver_license_number","driver_license_expiry","creation_date","is_active"
},
    "vehicles.csv": {"active","vehicle_license_number","registration_name","license_type","expiration_date","permit_license_number","certification_date","vehicle_year","base_telephone_number","base_address","vehicle_id","last_update_timestamp","brand","vehicle_type"},
    "locations.csv": {"location_id","location_name","address","city","state","zip_code","latitude","longitude"
},
    "rental_transactions.csv": {
      "rental_id","user_id","vehicle_id","rental_start_time","rental_end_time","pickup_location","dropoff_location","total_amount"
    },
}

# Entry point
def lambda_handler(event, context):
    s3 = boto3.client("s3")

    for file_name, expected_cols in REQUIRED_FILES.items():
        key = f"{PREFIX}{file_name}"
        logger.info(f"Validating {key}")

        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            content = obj["Body"].read().decode("utf-8").splitlines()
            reader = csv.reader(io.StringIO(content[0]))
            headers = set(next(reader))

            missing = expected_cols - headers
            unexpected = headers - expected_cols

            if missing or unexpected:
                msg = f"Schema mismatch in {file_name}. Missing: {missing}, Unexpected: {unexpected}"
                logger.error(msg)
                raise Exception(msg)
            else:
                logger.info(f"{file_name} passed schema validation.")

        except Exception as e:
            logger.error(f"Error processing {file_name}: {str(e)}")
            raise

    logger.info("All raw files passed validation.")
    return {"status": "success"}
