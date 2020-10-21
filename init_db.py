#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Initialize the geotweets SQLite3 database. """

from twitter_homes import db

def main():
    parser = argparse.ArgumentParser(
        description="initialize geotweets SQLite3 database"
    )
    parser.add_argument(
        "-d", "--db", required=True, help="the path of the database"
    )
    parser.add_argument(
        "inputs", nargs="+", help="paths to CSV files to be imported"
    )
    args = parser.parse_args()

    db.init_db(args.inputs, args.db)

if __name__ == "__main__":
    import argparse

    main()
