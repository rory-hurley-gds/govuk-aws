import boto3
import botocore
import os


def lambda_handler(event, context):
    rds_instance_name = os.environ["RDS_INSTANCE_NAME"]
    s3_bucket_name = os.environ["S3_BUCKET_NAME"]
    s3_prefix = os.environ["S3_BUCKET_PREFIX"]
    log_name_prefix = os.environ["LOG_NAME_PREFIX"]
    last_received_file = s3_prefix + os.environ["LAST_RECEIVED_FILE"]
    region = event["region"]

    s3_client = boto3.client("s3", region_name=region)
    rds_client = boto3.client("rds", region_name=region)
    db_logs = rds_client.describe_db_log_files(
        DBInstanceIdentifier=rds_instance_name, FilenameContains=log_name_prefix
    )
    time_copied_up_to = 0
    time_copied_up_to_this_run = 0
    writes = 0
    log_file_data = ""

    try:
        s3_client.head_bucket(Bucket=s3_bucket_name)
    except botocore.exceptions.ClientError as e:
        raise Exception(f"Unable to access bucket {s3_bucket_name}: {e}")

    try:
        lrf = s3_client.get_object(Bucket=s3_bucket_name, Key=last_received_file)
        time_copied_up_to = int(lrf["Body"].read())
        print(
            f"Found {last_received_file} from last log download; "
            f"retrieving log files with LastWritten time after {time_copied_up_to}"
        )
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response["ResponseMetadata"]["HTTPStatusCode"])
        if error_code == 404:
            print(
                f"{last_received_file} not found; seems this is the first run. "
                "All files will be retrieved from RDS."
            )
            time_copied_up_to = 0
        else:
            raise Exception(f"Unable to access {last_received_file}: {e}")

    for db_log in db_logs["DescribeDBLogFiles"]:
        if int(db_log["LastWritten"]) > time_copied_up_to:
            print(
                f"Downloading DB log file {db_log['LogFileName']} "
                f"(LastWritten={db_log['LastWritten']})"
            )

            if int(db_log["LastWritten"]) > time_copied_up_to_this_run:
                time_copied_up_to_this_run = int(db_log["LastWritten"])

            log_file = rds_client.download_db_log_file_portion(
                DBInstanceIdentifier=rds_instance_name,
                LogFileName=db_log["LogFileName"],
                Marker="0",
            )
            log_file_data = log_file["LogFileData"]

            while log_file["AdditionalDataPending"]:
                log_file = rds_client.download_db_log_file_portion(
                    DBInstanceIdentifier=rds_instance_name,
                    LogFileName=db_log["LogFileName"],
                    Marker=log_file["Marker"],
                )
                log_file_data += log_file["LogFileData"]
            byte_data = str.encode(log_file_data)

            try:
                obj_name = s3_prefix + db_log["LogFileName"]
                print(f"Attempting to write log file {obj_name} to S3 bucket {s3_bucket_name}")
                s3_client.put_object(Bucket=s3_bucket_name, Key=obj_name, Body=byte_data)
                writes += 1
                print(f"Successfully wrote log file {obj_name} to S3 bucket {s3_bucket_name}")
            except botocore.exceptions.ClientError as e:
                raise Exception(f"Error writing log file to S3 bucket, S3 ClientError: {e}")

    print("------------ Writing of files to S3 complete:")
    if writes:
        print(f"Successfully wrote {writes} log files.")
        try:
            s3_client.put_object(
                Bucket=s3_bucket_name,
                Key=last_received_file,
                Body=str.encode(str(time_copied_up_to_this_run)),
            )
            print(
                "Successfully wrote new last-written marker to "
                f"s3://{s3_bucket_name}/{last_received_file}"
            )
        except botocore.exceptions.ClientError as e:
            raise Exception(f"Error writing marker to S3 bucket, S3 ClientError: {e}")
    else:
        print("No new log files were written.")

    return "Log file export complete."
