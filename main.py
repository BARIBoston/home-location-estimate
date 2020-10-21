#!/usr/bin/env python3
""" Script to generate aggregate measures from Twitter data, including home
locations determined by DBSCAN* """

import csv
import os
import sqlite3
import typing

import pandas
import tqdm

from twitter_homes import measures, db

# expected keys in the output of run_all_measures(). we can determine this
# dynamically but it can get very expensive
OUTPUT_HEADERS = [
    "clustering_attempted", "n_dbscan_core_tweets", "n_dbscan_boundary_tweets",
    "n_dbscan_noise_tweets", "n_burst_tweets", "n_home_period_tweets",
    "home_cluster_id", "home_cluster_reason", "home_cluster_centroid_lon",
    "home_cluster_centroid_lat", "home_cluster_count", "most_frequent_name",
    "most_recent_name", "n_tweets", "n_tweets_in_ma", "unique_days", "user_id"
]

def run_all_measures(user_tweets_df: pandas.core.frame.DataFrame,
                     measures_: typing.List[typing.Callable] = measures.ALL_MEASURES
                     ) -> dict:
    """ Calculate aggregates measures.

    Args:
        user_tweets_df: A DataFrame containing Twitter data.
        measures_: A list of functions that return aggregate measures from df,
            each returning either a single value or a dict of values.

    Returns:
        A dict of calculated aggregate measures.
    """

    results: typing.Dict[str, typing.Union[str, int, float]] = {}
    for measure in measures_:
        return_value = measure(user_tweets_df)
        if not isinstance(return_value, dict):
            return_value = {measure.__name__: return_value}
        results = {**results, **return_value}
    return results

def main(db_paths: typing.Union[str, typing.List[str]],
         user_ids: typing.Iterable[int],
         output_file: str) -> None:
    """ Process users and write them to an output file """

    if os.path.isfile(output_file):
        with open(output_file, "r") as input_fp:
            seen_ids = set(
                int(row["user_id"])
                for row in csv.DictReader(input_fp)
            )
            print("skipping {} already-processed users".format(len(seen_ids)))
            user_ids = set(user_ids) - seen_ids
    else:
        with open(output_file, "w") as output_fp:
            writer = csv.DictWriter(output_fp, fieldnames=OUTPUT_HEADERS)
            writer.writeheader()

    # for some reason we can't use multiprocessing here because turicreate's
    # dbscan function doesn't work
    if len(db_paths) == 1:
        db_connector = sqlite3.connect
        db_paths = db_paths[0]
        print("using default sqlite3 connector")
    else:
        db_connector = db.MultiSqlite
        print("using MultiSqlite connector")

    with open(output_file, "a", 1) as output_fp,\
        db_connector(db_paths) as geotweets_db:

        writer = csv.DictWriter(output_fp, fieldnames=OUTPUT_HEADERS)

        for user_id in tqdm.tqdm(user_ids, desc="processing users"):
            user_tweets_df = pandas.read_sql_query(
                "SELECT * FROM geotweets WHERE user_id = {}".format(user_id),
                geotweets_db
            )
            if len(user_tweets_df) > 0:
                result = run_all_measures(user_tweets_df)
                if result:
                    writer.writerow(result)

if __name__ == "__main__":
    #pylint: disable=invalid-name

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i", "--input", required=True,
        help="the path to either a user ID or a file containing user IDs to"
              " process, with one user ID on each line."
    )
    parser.add_argument(
        "databases", nargs="+",
        help="geotweets databases to query; each database should have tweets"
             " in a table named \"geotweets\"."
    )
    parser.add_argument(
        "-o", "--output", required=True,
        help="the path to the CSV file to save outputs in."
    )
    args = parser.parse_args()

    if os.path.isfile(args.input):
        with open(args.input, "r") as f:
            user_ids = set(int(line.rstrip()) for line in f)
    else:
        user_ids = [args.input]

    main(args.databases, user_ids, args.output)
