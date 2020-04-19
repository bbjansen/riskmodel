"""Setup local mongodb."""

from pymongo import MongoClient
import glob
from bson.json_util import loads


def setup_mongodb(
    log,
    MONGO_DB_NAME: str = "legalthings",
    file_directory: list = ['data/preprocess/incorporation_processes/']
):
    """Creates MongoDB from local JSON files."""
    log.info("Start setup in {} mongodb".format(MONGO_DB_NAME))

    mongo_client = MongoClient('localhost', 27017)

    db = mongo_client[MONGO_DB_NAME]

    for directory in file_directory:
        all_files = list_files(directory, 'json')

        for file_name in all_files:
            collection_name = keep_only_file_name(file_name)
            db.collection_name.drop()
            new_collection = db[collection_name]

            incorporation_processes = []
            for line in open(file_name, 'r'):
                incorporation_processes.append(loads(line))

            new_collection.insert_many(incorporation_processes)
            log.info("New collection created: {}".format(collection_name))

    log.info("Finished creating all collections in {} mongodb"
             .format(MONGO_DB_NAME))


def list_files(
    directory: str = 'data/preprocess/incorporation_processes/',
    file_type: str = 'json'
) -> list:
    """Returns list of files in local folder."""
    return glob.glob("{}*.{}".format(directory, file_type))


def keep_only_file_name(
    file_name: str = 'data/preprocess/incorporation_processes/processes_0.json'
):
    """Returns only the file name without the extension."""
    return file_name.split('/')[-1].split('.')[0]
