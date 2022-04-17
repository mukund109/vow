import os
import boto3


def fetch_data():
    if os.path.isfile("vow.db"):
        return

    print("Fetching data from object storage...")
    session = boto3.session.Session()
    client = session.client(
        "s3",
        endpoint_url=os.environ["SPACES_URL"],
        region_name=os.environ["SPACES_REGION"],
        aws_access_key_id=os.environ["SPACES_KEY"],
        aws_secret_access_key=os.environ["SPACES_SECRET"],
    )

    client.download_file(os.environ["SPACES_BUCKET"], "vow.db", "vow.db")
