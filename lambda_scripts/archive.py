import boto3

def run_archive(source_bucket, folders):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(source_bucket)

    for folder in folders:
        source_prefix = folder.get("source_prefix")
        destination_prefix = folder.get("destination_prefix")

        if not source_prefix or not destination_prefix:
            continue

        print(f"Processing folder: {source_prefix} → {destination_prefix}")

        for obj in bucket.objects.filter(Prefix=source_prefix):
            src_key = obj.key
            if src_key.endswith("/"):
                continue

            dst_key = src_key.replace(source_prefix, destination_prefix, 1)

            # Copy then delete
            copy_source = {'Bucket': source_bucket, 'Key': src_key}
            s3.Object(source_bucket, dst_key).copy_from(CopySource=copy_source)
            s3.Object(source_bucket, src_key).delete()

            print(f"Moved {src_key} → {dst_key}")

    return {
        "statusCode": 200,
        "message": f"Archived {len(folders)} folder(s) successfully."
    }
