"""AWS data access."""

from dotenv import load_dotenv
import os
import boto3
import botocore
import re


def with_aws_service(f):
    """Decorate function to access aws service."""
    def wrap(*args, **kwargs):
        load_dotenv()
        s3 = boto3.resource('s3')
        kwargs['s3'] = s3
        return f(*args, **kwargs)
    return wrap


def make_folder(path):
    """Create folder."""
    if not os.path.exists(path):
        os.makedirs(path)


def extract_filename_from_filepath(filepath, file_type):
    """Extract filename from full filepath."""
    for file_type in file_type:
        filename = re.search('/(.*).{}'.format(file_type), filepath)

        if filename:
            return filename.group(1).split('/')[-1] + ".{}".format(file_type)


@with_aws_service
def download_files_remote_to_local(
    log, bucket: str,
    remote: str, local: str, file_type: list, *args, **kwargs
):
    """Downloads files from AWS bucket in remote folder and
    puts them in the local folder."""
    make_folder(os.path.join(local, remote))
    s3 = kwargs['s3']

    files = list_files(bucket, remote, file_type)

    for file in files:
        try:
            log.info(f"Downloading: {file}")
            file_name = extract_filename_from_filepath(file, file_type)
            local_filename = os.path.join(local, remote, file_name)
            s3.Bucket(bucket).download_file(file, local_filename)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise


@with_aws_service
def list_files(
    bucket: str, remote: str, file_type: list, *args, **kwargs
) -> list:
    """Returns list of files in remote folder."""
    s3 = kwargs['s3']

    files = []

    my_bucket = s3.Bucket(bucket)

    for file in my_bucket.objects.all():
        if file.key.startswith(remote) and any(
                file.key.endswith('.{}'.format(ext))
                for ext in file_type):
            files.append(file.key)
    return files
