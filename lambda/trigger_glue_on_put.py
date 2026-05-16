"""S3 PutObject -> Glue job start. Wired in Terraform via EventBridge."""
import os

import boto3


glue = boto3.client("glue")
JOB = os.environ.get("GLUE_JOB_NAME", "hmis_etl_job")


def handler(event, _ctx):
    rec = event["Records"][0]["s3"]
    src = f"s3://{rec['bucket']['name']}/{rec['object']['key']}"
    return glue.start_job_run(JobName=JOB, Arguments={"--SRC_PATH": src})
