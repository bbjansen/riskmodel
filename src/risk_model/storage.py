
from dotenv import load_dotenv
import os
import boto3
import botocore
import re


def with_aws_service(f):
    """Decorate function to add aws service."""
    def wrap(*args, **kwargs):
        load_dotenv()
        ACCESS_KEY = os.getenv('STORAGE_ACCOUNT_NAME')
        SECRET_KEY = os.getenv('STORAGE_ACCOUNT_KEY')

        client = boto3.client(
            's3',
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
        )
        s3 = boto3.resource('s3')
        kwargs['s3'] = s3
        return f(*args, **kwargs)
    return wrap


def make_folder(path):
    """Create folder."""
    if not os.path.exists(path):
        os.makedirs(path)


def extract_filename_from_filepath(filepath):
    """Extract filename from full filepath."""
    return (
        re
        .search('/(.*).csv', filepath)
        .group(1)
        .split('/')
    )[-1]


@with_aws_service
def download_csv_files(
    bucket: str, remote: str, local: str, *args, **kwargs
):
    make_folder(os.path.join(local, remote))
    s3 = kwargs['s3']

    files = list_csv_files(bucket, remote)

    for file in files:
        try:
            file_name = extract_filename_from_filepath(file)
            local_filename = os.path.join(local, remote, file_name)
            s3.Bucket(bucket).download_file(file, local_filename)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise


@with_aws_service
def list_csv_files(
    bucket: str, remote: str, *args, **kwargs
) -> list:
    """Returns list of csv files in remote folder."""
    s3 = kwargs['s3']
    files = []

    my_bucket = s3.Bucket(bucket)

    for file in my_bucket.objects.all():
        if remote in file.key and file.key.endswith('.csv'):
            files.append(file.key)

    return files
