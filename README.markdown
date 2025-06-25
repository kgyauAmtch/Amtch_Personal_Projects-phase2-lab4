# AWS EMR and Step Function Pipeline

This project implements an AWS-based data processing pipeline using Elastic MapReduce (EMR), AWS Step Functions, AWS Glue, and Amazon Athena to process and analyze rental transaction data. The pipeline performs data transformation, calculates key performance indicators (KPIs), catalogs the results, and queries them using Athena.


## Overview
The pipeline processes raw rental data (users, vehicles, rentals, and locations) stored in Amazon S3. It uses AWS EMR to run two Spark jobs: one for data transformation and another for KPI calculation. The transformed data and KPIs are saved as Parquet files, cataloged using AWS Glue, and queried using Amazon Athena. AWS Step Functions orchestrates the entire workflow, ensuring fault tolerance and parallel execution where applicable.

## Architecture
The pipeline consists of the following AWS services:
- **Amazon S3**: Stores raw input data, transformed data, KPI outputs, logs, and Athena query results.
- **AWS EMR**: Runs Spark jobs for data transformation and KPI calculation.
- **AWS Step Functions**: Orchestrates the pipeline, managing cluster creation, job execution, and cleanup.
- **AWS Glue**: Catalogs the Parquet outputs for querying.
- **Amazon Athena**: Executes SQL queries on the cataloged data.
- **AWS Lambda**: Handles Athena query execution.

   ![Architecture diagram](Architecture.png)



## Prerequisites
- **S3 Bucket** (`lab4-bucket-gtp`) with the following structure:
  - `s3://lab4-bucket-gtp/input/` for raw CSV data (users.csv, rental_transactions.csv, vehicles.csv, locations.csv).
  - `s3://lab4-bucket-gtp/emr-scripts/` for Spark job scripts (job1.py, job2.py).
  - `s3://lab4-bucket-gtp/output/` for transformed data and KPI outputs.
  - `s3://lab4-bucket-gtp/athena_results/` for Athena query results.
  - `s3://lab4-bucket-gtp/logs/` for EMR logs.
- **IAM Roles**:
  - `EMR_DefaultRole-lab4new` for EMR service.
  - `EMR_EC2_DefaultRole-lab4` for EMR EC2 instances.
  - Roles for Step Functions, Glue, and Lambda with appropriate permissions (e.g., S3, EMR, Glue, Athena).
- **AWS Glue Crawler** (`lab4emr-1`) configured to catalog Parquet outputs in `s3://lab4-bucket-gtp/output/`.
- **AWS Lambda Function** (`athena-lambda`) for Athena query execution.
- **Input Data** in CSV format with schemas matching those defined in `transformation_job.py`.

## Setup Instructions
1. **Create S3 Bucket**:
   - Create `lab4-bucket-gtp` with the required folder structure.
   - Upload raw CSV files to `s3://lab4-bucket-gtp/input/`.
   - Upload `transformation_job.py` and `kpi_calculation.py` to `s3://lab4-bucket-gtp/emr-scripts/`.

2. **Configure IAM Roles**:
   - Ensure `EMR_DefaultRole-lab4new` and `EMR_EC2_DefaultRole-lab4` exist with policies for S3, EMR, and CloudWatch.
   - Create a Lambda role with permissions for Athena and S3.
   - Create a Step Functions role with permissions for EMR, Glue, Lambda, and S3.

3. **Deploy Lambda Function**:
   - Create a Lambda function named `athena-lambda` using `lambda_function.py`.
   - Configure the function to use Python 3.9+ and attach the appropriate IAM role.

4. **Set Up AWS Glue**:
   - Create a Glue Crawler (`lab4emr-1`) to catalog Parquet files in `s3://lab4-bucket-gtp/output/`.
   - Configure the crawler to target the `lab4emr-db` database.

5. **Deploy Step Function**:
   - Create a Step Function using `step_function.json`.
   - Ensure the state machine references the correct ARNs and resources.

6. **Validate Input Data**:
   - Ensure CSV files in `s3://lab4-bucket-gtp/input/` match the schemas defined in `transformation_job.py`.

## Pipeline Workflow
The pipeline is orchestrated by AWS Step Functions and consists of the following steps:
1. **Create EMR Cluster**: Launches an EMR cluster with Spark support (1 master, 2 core nodes, m5.xlarge instances).
2. **Run Spark Jobs in Parallel**:
   - **Job 1 (transformation_job.py)**: Transforms raw CSV data into Parquet format with cleaning and standardization.
   - **Job 2 (kpi_calculation.py)**: Calculates KPIs (e.g., revenue, transaction counts, rental durations) from transformed data.
3. **Terminate EMR Cluster**: Shuts down the cluster after job completion.
4. **Wait**: Adds a 10-second delay before triggering the Glue Crawler.
5. **Trigger Glue Crawler**: Runs the crawler to catalog Parquet outputs.
6. **Wait**: Adds a 5-second delay before querying with Athena.
7. **Query in Athena**: Invokes the Lambda function to execute an Athena query and save results to S3.
8. **Fail State**: Handles errors if any step fails.


## Data Flow
1. **Input**: Raw CSV files (`users.csv`, `rental_transactions.csv`, `vehicles.csv`, `locations.csv`) in `s3://lab4-bucket-gtp/input/`.
2. **Transformation**: `transformation_job.py` reads CSVs, applies transformations, and writes Parquet files to `s3://lab4-bucket-gtp/output/`.
3. **KPI Calculation**: `kpi_calculation.py` reads transformed Parquet files, computes KPIs, and writes results to `s3://lab4-bucket-gtp/output/`.
4. **Cataloging**: AWS Glue Crawler catalogs Parquet files into the `lab4emr-db` database.
5. **Querying**: Athena queries the cataloged data via `lambda_function.py`, saving results to `s3://lab4-bucket-gtp/athena_results/`.

