import boto3
import time

def lambda_handler(event, context):
    athena = boto3.client('athena')

    # --- Parameters ---
    database = "lab4emr-db" 
    output_location = "s3://lab4-bucket-gtp/athena_results/"  # must exist
    query = "SELECT * FROM daily_metrics LIMIT 10;"  # adjust as needed

    # --- Submit Query ---
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_location}
    )
    query_execution_id = response['QueryExecutionId']

    # --- Wait for Completion ---
    while True:
        result = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = result['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)

    if state != 'SUCCEEDED':
        raise Exception(f"Athena query failed with state: {state}")

    return {
        "statusCode": 200,
        "queryExecutionId": query_execution_id,
        "outputFile": f"{output_location}{query_execution_id}.csv"
    }
