"""Setup local mongodb."""

from pymongo import MongoClient
import pandas as pd
from pandas import json_normalize
from risk_model.storage import make_folder
import os


def run(
    log,
    MONGO_DB_NAME: str = "legalthings",
    collection_name_like: str = 'processes',
    features: dict = {
        '_id': 1,
        'name': 1,
        'creation': 1,
        'current.title': 1,
        'current.activation_date': 1,
        'current.actor.email': 1,
        'current.actor.name': 1,
        'private_data.incorporation_type': 1},
    nested_features: list = ['private_data.meta_data.ah'],
    pandas_location: str = 'data/pandas/',
    pandas_file_name: str = 'features.pkl'
):
    """Creates pandas dataframe from mongo collection(s).

    params:
        - 'MONGO_DB_NAME' (string): name of database Mongodb.
        - 'collection_name_like' (string): string is part of collection name.
        - 'features' (dict): unnested JSON key(s) in document.
        - 'nested_features' (list): nested JSON structure in document.
        - 'pandas_location' (str): directory name where to save the pandas dataframe.
        - 'pandas_file_name' (str): filename of pandas dataframe.

    return:
        Saves a pandas-dataframe with the (nested) features extracted as columns.

    """
    log.info("Load {} mongodb".format(MONGO_DB_NAME))

    mongo_client = MongoClient('localhost', 27017)
    db = mongo_client[MONGO_DB_NAME]

    all_collections = db.list_collection_names()
    all_feature_df = None

    for collection_name in all_collections:
        if collection_name_like in collection_name:

            # Extract unnested JSON features
            df = (
                json_normalize(
                    db[
                        collection_name].find({}, features))
            )

            # Extract nested JSON features
            for nested in nested_features:
                cursor = db[
                    collection_name].find({nested: {"$exists": True}})

                nested_df = json_normalize(
                    cursor, [nested.split('.')], ['_id'])

                # Join unnested and nested features together
                features_df = pd.merge(df, nested_df, on='_id', how='left')

                # Concat features of all collections together
                if all_feature_df is None:
                    all_feature_df = features_df
                else:
                    all_feature_df = pd.concat([all_feature_df, features_df])

                log.info("Collection processed as dataframe: {}".format(
                    collection_name))

    # Save pandas dateframe
    make_folder(pandas_location)
    file_name = os.path.join(pandas_location, pandas_file_name)
    all_feature_df.to_pickle(file_name)
    log.info("Saved features as pandas dataframe in: {}".format(file_name))
