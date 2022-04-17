import os
import boto3

session = boto3.session.Session()
client = session.client(
    "s3",
    endpoint_url=os.environ["SPACES_URL"],
    region_name=os.environ["SPACES_REGION"],
    aws_access_key_id=os.environ["SPACES_KEY"],
    aws_secret_access_key=os.environ["SPACES_SECRET"],
)

client.download_file("SPACES_BUCKET"], "vow.db", "vow.db")
