""" Calculate aggregate measures for Twitter users.

Warning: all periods in the CSV files become underscores in the SQLite
database, in order to conform to proper naming standards.
"""

import inspect
import os
import sys

import pandas
import shapely.wkb

from . import dbscan

REQUIRED_COLUMNS = {
    "created_at", "coordinates.coordinates.0", "coordinates.coordinates.1",
    "user.id", "user.name",
}
LON_LAT_COLUMNS = ["coordinates_coordinates_0", "coordinates_coordinates_1"]

MA_BOUNDS = os.path.join(os.path.dirname(__file__), "data", "massachusetts.wkb")

with open(MA_BOUNDS, "rb") as f:
    MA_SHAPE = shapely.wkb.load(f)

def user_id(tweets_df: pandas.core.frame.DataFrame) -> str:
    """ Get the user ID. """
    return tweets_df.iloc[0]["user_id"]

def most_frequent_name(tweets_df: pandas.core.frame.DataFrame) -> str:
    """ Get the most frequently appearing user name. """
    return tweets_df["user_name"].mode().get(0)

def most_recent_name(tweets_df: pandas.core.frame.DataFrame) -> str:
    """ Get the most recently appearing user name. """
    return tweets_df.iloc[-1]["user_name"]

def n_tweets(tweets_df: pandas.core.frame.DataFrame) -> int:
    """ Get the number of tweets. """
    return len(tweets_df)

def n_tweets_in_ma(tweets_df: pandas.core.frame.DataFrame) -> int:
    """ Get the number of tweets appearing within the bounds of Boston. """
    return tweets_df[LON_LAT_COLUMNS].apply(
        lambda coords: shapely.geometry.Point(coords).within(MA_SHAPE),
        axis = 1
    ).sum()

def dbscan_results(tweets_df: pandas.core.frame.DataFrame) -> dict:
    """ Return the results of DBSCAN (see dbscan module for more info) """
    return dbscan.dbscan(tweets_df)

def unique_days(tweets_df: pandas.core.frame.DataFrame) -> int:
    """  Return the number of unique days that this user appears on. """
    return len(
        tweets_df["created_at"].apply(
            # First 3 words are: shortened weekday, month, day of month
            # This is unique for each day
            lambda date_str: tuple(date_str.split(" ")[:3])
        ).unique()
    )

ALL_MEASURES = [
    obj
    for (name, obj) in inspect.getmembers(sys.modules[__name__])
    if callable(obj)
]