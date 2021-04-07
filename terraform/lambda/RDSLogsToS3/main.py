import boto3
import botocore
import os


def lambda_handler(event, context):
    RDSInstanceName = os.environ["RDS_INSTANCE_NAME"]
    S3BucketName = os.environ["S3_BUCKET_NAME"]
    S3BucketPrefix = os.environ["S3_BUCKET_PREFIX"]
    logNamePrefix = os.environ["LOG_NAME_PREFIX"]
    lastReceivedFile = S3BucketPrefix + os.environ["LAST_RECEIVED_FILE"]
    region = event["region"]

    S3client = boto3.client("s3", region_name=region)
    RDSclient = boto3.client("rds", region_name=region)
    dbLogs = RDSclient.describe_db_log_files(
        DBInstanceIdentifier=RDSInstanceName, FilenameContains=logNamePrefix
    )
    lastWrittenTime = 0
    lastWrittenThisRun = 0
    writes = 0
    logFileData = ""

    try:
        S3client.head_bucket(Bucket=S3BucketName)
    except botocore.exceptions.ClientError as e:
        raise Exception(f"Unable to access bucket {S3BucketName}: {e}")

    try:
        lrfHandle = S3client.get_object(Bucket=S3BucketName, Key=lastReceivedFile)
        lastWrittenTime = int(lrfHandle["Body"].read())
        print(
            f"Found {lastReceivedFile} from last log download; "
            f"retrieving log files with lastWritten time after {lastWrittenTime}"
        )
    except botocore.exceptions.ClientError as e:
        errorCode = int(e.response["ResponseMetadata"]["HTTPStatusCode"])
        if errorCode == 404:
            print(
                f"{lastReceivedFile} not found; seems this is the first run. "
                "All files will be retrieved from RDS."
            )
            lastWrittenTime = 0
        else:
            raise Exception(f"Unable to access {lastReceivedFile}: {e}")

    for dbLog in dbLogs["DescribeDBLogFiles"]:
        if int(dbLog["LastWritten"]) > lastWrittenTime:
            print(
                f"Downloading DB log file {dbLog['LogFileName']} "
                f"(LastWritten={dbLog['LastWritten']})"
            )

            if int(dbLog["LastWritten"]) > lastWrittenThisRun:
                lastWrittenThisRun = int(dbLog["LastWritten"])

            logFile = RDSclient.download_db_log_file_portion(
                DBInstanceIdentifier=RDSInstanceName,
                LogFileName=dbLog["LogFileName"],
                Marker="0",
            )
            logFileData = logFile["LogFileData"]

            while logFile["AdditionalDataPending"]:
                logFile = RDSclient.download_db_log_file_portion(
                    DBInstanceIdentifier=RDSInstanceName,
                    LogFileName=dbLog["LogFileName"],
                    Marker=logFile["Marker"],
                )
                logFileData += logFile["LogFileData"]
            byteData = str.encode(logFileData)

            try:
                objectName = S3BucketPrefix + dbLog["LogFileName"]
                print(f"Attempting to write log file {objectName} to S3 bucket {S3BucketName}")
                S3client.put_object(Bucket=S3BucketName, Key=objectName, Body=byteData)
                writes += 1
                print(f"Successfully wrote log file {objectName} to S3 bucket {S3BucketName}")
            except botocore.exceptions.ClientError as e:
                raise Exception(f"Error writing log file to S3 bucket, S3 ClientError: {e}")

    print("------------ Writing of files to S3 complete:")
    if writes:
        print(f"Successfully wrote {writes} log files.")
        try:
            S3client.put_object(
                Bucket=S3BucketName,
                Key=lastReceivedFile,
                Body=str.encode(str(lastWrittenThisRun)),
            )
            print(
                "Successfully wrote new last-written marker to "
                f"s3://{S3BucketName}/{lastReceivedFile}"
            )
        except botocore.exceptions.ClientError as e:
            raise Exception(f"Error writing marker to S3 bucket, S3 ClientError: {e}")
    else:
        print("No new log files were written.")

    return "Log file export complete."
