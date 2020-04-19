"""Main module."""

import argparse
import logging
import sys
import shutil
import os

from dotenv import load_dotenv
from risk_model import storage, preprocess, mongodb

format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

logging.basicConfig(stream=sys.stdout,
                    level=logging.INFO,
                    format=format)


BUCKET_NAME = 'legalthings-datalake'
AWS_FILEPATHS = {
    'mongo/': ['csv', 'json']
}

MONGO_DB_NAME = "legalthings"


def get_args():
    """Parse potential user arguments."""
    parser = argparse.ArgumentParser('riskmodel')
    return parser.parse_args()


def run(
    log,
    download=False,
    parse_json=True
):
    """Run job."""

    # Step 1: Download files from AWS S3.

    if download:
        log.info("Starting collect data")

        args = get_args()
        log.info(f"Parsing args: {args}")

        log.info("Loading env vars")
        load_dotenv('env_file')

        log.info(f"Downloading and saving latest files")

        for remote, file_type in AWS_FILEPATHS.items():
            log.info(f"Downloading: {remote}{file_type} files")
            storage.download_files_remote_to_local(
                log,
                bucket=BUCKET_NAME,
                remote=remote,
                local='data',
                file_type=file_type)
            log.info(f"Finished download and saved {remote} in data folder")
        log.info("Finished collecting data")

    # Step 2: Parse JSON to readable and memorable format.

    if parse_json:
        log.info("Start parsing readable JSON files")

        # Clear directory
        clear_dir = 'data/preprocess/incorporation_processes/'
        if os.path.exists(clear_dir):
            shutil.rmtree('data/preprocess/incorporation_processes/')

        # Parse big JSON from onliner to multiple, python readable JSON lines
        preprocess.make_readable_json(
            unprocessed_filename='data/mongo/incorporation-processes.json',
            preprocessed_dir='data/preprocess/incorporation_processes/',
            split_variabele='{"_id":'
        )
        log.info("Parsed onliner to multiple, python readable JSON lines")

        # Split big json file into separate, python memorable json files
        preprocess.split_file_into_multiple_files(
            directory='data/preprocess/incorporation_processes',
            file_to_split='incorporation-processes.json',
            new_sub_file_name='processes',
            n_files=35,
            file_type='json'
        )

        # Check total number of lines sum up
        test_total_lines_subfiles(
            directory='data/preprocess/incorporation_processes',
            file_before_split='incorporation-processes.json',
            new_sub_file_name='processes',
            n_files=35
        )

        os.remove(os.path.join('data/preprocess/incorporation_processes',
                               'incorporation-processes.json'))
        log.info("Finished splitting big json into separate files")

    # Step 3: Create MongoDB from JSON subfiles.
    mongodb.setup_mongodb(
        log=log,
        MONGO_DB_NAME=MONGO_DB_NAME,
        file_directory=['data/preprocess/incorporation_processes/']
    )

    log.info("Finished job.")
    return 0


def test_total_lines_subfiles(
    directory='data/preprocess/incorporation_processes',
    file_before_split='incorporation-processes.json',
    new_sub_file_name='processes',
    n_files=35
):
    """Validates if line count orginal BIG file sums up
    with line count of all subfiles together."""
    line_count_orginal = sum(
        1 for line in open(os.path.join(directory,
                                        file_before_split)))

    line_count = 0

    for i in range(0, n_files):
        file_name = os.path.join(directory,
                                 '{}_{}.json'.format(new_sub_file_name, i))
        line_count += sum(1 for line in open(file_name))

    assert(line_count_orginal == line_count)


if __name__ == "__main__":
    log = logging.getLogger(__name__)
    try:
        sys.exit(run(log))
    except KeyboardInterrupt:
        print("\nYou stopped the program.")
