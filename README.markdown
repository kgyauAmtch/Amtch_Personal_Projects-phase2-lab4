# EMR Step Function Pipeline

This project implements an AWS Step Function pipeline to process data using an EMR cluster, run Spark jobs, catalog results with AWS Glue, and query with Athena.

## Overview
The pipeline:
1. Creates a temporary EMR cluster with Spark.
2. Runs two Spark jobs in parallel:
   - **Job 1** (`transformation_job.py`): Transforms raw CSV data into Parquet format.
   - **Job 2** (`kpis.py`): Calculates KPIs from transformed data.
3. Terminates the EMR cluster.
4. Triggers a Glue Crawler to catalog the output.
5. Queries the results using Athena via a Lambda function.

Architecture Diagram
![Architecture Diagram](images/Architecture.svg)

## Components
- **step_function.json**: Defines the Step Function workflow to orchestrate the pipeline.
- **transformation_job.py**: Spark job to clean and transform raw CSV data (users, rentals, vehicles, locations) into Parquet.
- **kpis.py**: Spark job to compute KPI metrics (location, vehicle, transaction, user) from transformed data.
- **lambda_function.py**: Lambda function to execute an Athena query on the `daily_metrics` table.

## Prerequisites
- AWS account with permissions for EMR, Step Functions, Glue, Athena, S3, and Lambda.
- S3 bucket (`lab4-bucket-gtp`) with subfolders:
  - `raw/`: Input CSV files (`users.csv`, `rental_transactions.csv`, `vehicles.csv`, `locations.csv`).
  - `emr-scripts/`: Spark scripts (`transformation_job.py`, `kpis.py`).
  - `output/`: Transformed Parquet data.
  - `kpi_output/`: KPI metrics in Parquet.
  - `logs/`: EMR logs.
  - `athena_results/`: Athena query results.
- Glue Crawler (`lab4emr-1`) configured for the output data.
- Glue Database (`lab4emr-db`) for cataloging.
- Lambda function (`athena-lambda`) with permissions to run Athena queries.
- IAM roles: `EMR_DefaultRole-lab4new`, `EMR_EC2_DefaultRole-lab4`.

## Setup
1. **Upload Scripts**: Place `transformation_job.py` and `kpis.py` in `s3://lab4-bucket-gtp/emr-scripts/`.
2. **Configure S3**: Ensure input CSVs are in `s3://lab4-bucket-gtp/raw/` and other subfolders exist.
3. **Deploy Step Function**: Create a Step Function using `step_function.json`.
4. **Set Up Glue**: Configure the Glue Crawler to catalog data in `s3://lab4-bucket-gtp/kpi_output/`.
5. **Deploy Lambda**: Deploy `lambda_function.py` as `athena-lambda` with appropriate IAM permissions.

## Execution
1. Start the Step Function execution via AWS Console or CLI.
2. Monitor the pipeline in the Step Function console.
3. Check Athena query results in `s3://lab4-bucket-gtp/athena_results/`.

## Outputs
- Transformed data: `s3://lab4-bucket-gtp/output/{vehicles,users,rentals,locations}/`
- KPI metrics: `s3://lab4-bucket-gtp/kpi_output/{location_metrics,vehicle_metrics,transaction_metrics,user_metrics}/`
- Athena results: `s3://lab4-bucket-gtp/athena_results/<query_execution_id>.csv`

## Notes
- The EMR cluster is terminated after job completion or on failure.
- Wait states (10s, 5s) ensure Glue and Athena operations run smoothly.
- Error handling routes failures to terminate the cluster and fail the workflow.
- Adjust the Athena query in `lambda_function.py` as needed.

## Troubleshooting
- Check EMR logs in `s3://lab4-bucket-gtp/logs/` for Spark job issues.
- Verify IAM roles and S3 permissions.
- Ensure the Glue Crawler has run successfully before Athena queries.