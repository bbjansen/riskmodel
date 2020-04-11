"""Main module."""

import argparse
import logging
import os
import sys

from dotenv import load_dotenv
from risk_model import storage

format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

logging.basicConfig(stream=sys.stdout,
                    level=logging.INFO,
                    format=format)


def get_args():
    """Parse potential user arguments."""
    parser = argparse.ArgumentParser('riskmodel')
    return parser.parse_args()


def run(log):
    """Run job."""
    log.info("Starting collect data")

    args = get_args()
    log.info(f"Parsing args: {args}")

    log.info("Loading env vars")
    load_dotenv('env_file')

    log.info(f"Downloading and saving latest files")
    cfgs = storage.download_csv_files(
        bucket='legalthings-datalake',
        remote='datastudio/BVshareholders',
        local='data')

    log.info("Finished collecting data")
    return 0


if __name__ == "__main__":
    log = logging.getLogger(__name__)
    try:
        sys.exit(run(log))
    except KeyboardInterrupt:
        print("\nYou stopped the program.")
