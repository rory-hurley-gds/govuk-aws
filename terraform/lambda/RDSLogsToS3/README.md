## Send RDS logs to S3

This Lambda function retrieves RDS logs and stores them in the S3 logging bucket. It's
based on https://github.com/vcardillo/rdslogs_to_s3

It is triggered by a CloudWatch scheduled event.

### Testing locally

1. Install [AWS sam-local](https://github.com/awslabs/aws-sam-local):

    - `brew tap aws/tap`
    - `brew install aws-sam-cli`
    - Install Docker if you don't already have it.

2. Generate event JSON:

        sam local generate-event cloudwatch scheduled-event --region eu-west-1 > event.json

3. Configure the following environment variables in `template.yaml`:

    - RDS_INSTANCE_NAME: RDS test instance name (e.g. `blue-transition-postgresql-standby`)
    - S3_BUCKET_NAME: logging bucket name (e.g. `govuk-integration-aws-logging`)
    - S3_BUCKET_PREFIX: rds/<rds_instance_name>/

4. Test the Lambda function:

        sam local invoke RDSLogsToS3 -e event.json
