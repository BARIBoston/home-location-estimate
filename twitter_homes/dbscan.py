#!/usr/bin/env python3

import dateutil.parser
import numpy
import os
import pandas
import pytz
import timezonefinder
import turicreate

EPS = 0.0004
MIN_POINTS = 3
RADIUS_OF_EARTH = 6372800 # meters

VERBOSE = False

# TODO: maybe have these defined in a configuration file
CLUSTERS_OUTPUT = "output_clustering_results"
AGGREGATES_OUTPUT = "output_cluster_aggregates"

REQUIRED_COLUMNS = {
    "created_at", "coordinates.coordinates.0", "coordinates.coordinates.1",
    "user.id"
}
LON_COLUMN = "coordinates_coordinates_0"
LAT_COLUMN = "coordinates_coordinates_1"
BURST_TWEETS_GROUPBY_COLUMNS = ["created_at", LON_COLUMN, LAT_COLUMN]
DBSCAN_CLUSTER_TYPES = ["core", "boundary", "noise"]

def print_verbose(str_):
    if (VERBOSE):
        print(str_)

def merge_dicts(*dicts):
    new_dict = dicts[0].copy()
    for dict_ in dicts[1:]:
        new_dict.update(dict_)
    return new_dict

class DatetimeLocalizer(object):

    def __init__(self):
        self.timezones = {}
        self.timezonefinder = timezonefinder.TimezoneFinder()

    def lookup_tz(self, lon, lat):
        tz_name = self.timezonefinder.timezone_at(lng = lon, lat = lat)
        if (not tz_name in self.timezones):
            try:
                self.timezones[tz_name] = pytz.timezone(tz_name)
            except AttributeError:
                print("TZ ERROR: (%s, %s)" % (lon, lat))
                self.timezones[tz_name] = None
        return self.timezones[tz_name]

    def get_localized_dt(self, tweet):
        # TODO: replace with snowflake
        dt = dateutil.parser.parse(tweet["created_at"])
        try:
            tz = self.lookup_tz(*tweet[[LON_COLUMN, LAT_COLUMN]])
            if (tz is None):
                return dt
            else:
                return dt.astimezone(tz)
        except:
            return dt

# From https://stackoverflow.com/a/45395941
def haversine(lat1, lon1, lat2, lon2):
    dLat = numpy.radians(lat2 - lat1)
    dLon = numpy.radians(lon2 - lon1)
    lat1 = numpy.radians(lat1)
    lat2 = numpy.radians(lat2)
    a = numpy.sin(dLat/2)**2 + numpy.cos(lat1)*numpy.cos(lat2)*numpy.sin(dLon/2)**2
    c = 2*numpy.arcsin(numpy.sqrt(a))
    return RADIUS_OF_EARTH * c

def dist_euclidean(x1, y1, x2, y2):
    return numpy.sqrt((y2 - y1)**2 + (x2 - x1)**2)

def coords_to_xy(coords):
    return (coords.x, coords.y)

def create_output_df(df_clusters):
    df_output = pandas.DataFrame()
    df_output["datetime"] = df_clusters["dt"].apply(lambda dt: dt.isoformat())
    df_output["tz_name"] = df_clusters["dt"].apply(lambda dt: dt.tzname())
    df_output["user_id"] = df_clusters["user_id"]
    df_output["lon"] = df_clusters[LON_COLUMN]
    df_output["lat"] = df_clusters[LAT_COLUMN]
    df_output["cluster_id"] = df_clusters["cluster_id"]
    df_output["type"] = df_clusters["type"]
    return df_output

def create_cluster_aggregates(df_clusters):
    clusters = []

    for cluster_id in df_clusters["cluster_id"].unique():
        this_cluster = df_clusters[df_clusters["cluster_id"] == cluster_id]
        coordinates = this_cluster[[LON_COLUMN, LAT_COLUMN]]
        cluster_centroid = coordinates.mean()
        clusters.append({
            "cluster_id": cluster_id,
            "centroid_lon": cluster_centroid[0],
            "centroid_lat": cluster_centroid[1],
            "time_range": (
                this_cluster["dt"].max() - this_cluster["dt"].min()
            ).total_seconds(),
            "max_dist_from_centroid": haversine(
                cluster_centroid[0], cluster_centroid[1],
                coordinates[LON_COLUMN].values, coordinates[LAT_COLUMN].values
            ).max()
        })

    # For cluster counts, we merge a df.value_counts() table on its index
    cluster_aggregates = pandas.merge(
        pandas.DataFrame(clusters),
        df_clusters["cluster_id"].value_counts()\
            .rename_axis("cluster_id").reset_index(name = "count"),
        on = "cluster_id"
    ).sort_values("cluster_id").reset_index(drop = True)
    cluster_aggregates["count"] = cluster_aggregates["count"]

    return cluster_aggregates

# Return the cluster_id corresponding to the max/min/etc value of a column in a
# DataFrame if it is unique, or None if it isn't
def unique_cluster_id(cluster_aggregates, column, measure):
    value = measure(cluster_aggregates[column])
    counts = cluster_aggregates[column].value_counts()
    if (counts.loc[value] == 1):
        return cluster_aggregates[
            cluster_aggregates[column] == value
        ]["cluster_id"].values[0]

def determine_home_cluster(cluster_aggregates):
    for (column, measure, reason) in [
        # Case 1: Only one cluster to consider home
        ("count", max, "only one max cluster"),

        # Case 2: Several clusters with different dispersions
        # Choose the one with the least dispersion
        ("max_dist_from_centroid", min, "cluster with minimum distance"),

        # Case 3: Several clusters with different date ranges
        # Choose the one with the largest date range
        ("time_range", max, "cluster with maximum time range")
    ]:
        home_cluster_id = unique_cluster_id(
            cluster_aggregates, column, measure
        )
        if (home_cluster_id is not None):
            return (home_cluster_id, reason)
    return (None, None)

def dbscan(tweets_df):
    df = tweets_df.copy()
    user_id = df.loc[0]["user_id"]
    localizer = DatetimeLocalizer()
    dbscan_debug_info = {
        "clustering_attempted": 0
    }
    dbscan_debug_info.update({
        "n_dbscan_%s_tweets" % cluster_type: None
        for cluster_type in DBSCAN_CLUSTER_TYPES
    })

    for directory in [CLUSTERS_OUTPUT, AGGREGATES_OUTPUT]:
        if not os.path.isdir(directory):
            os.makedirs(directory)

    ## PREPROCESSING ###########################################################

    # Removal of burst tweets
    n_orig = len(df)
    df = df.groupby(BURST_TWEETS_GROUPBY_COLUMNS).first().reset_index()
    n_deduplicated = len(df)
    dbscan_debug_info["n_burst_tweets"] = n_orig - n_deduplicated

    # Localize datetimes
    print_verbose("Localizing datetimes")
    df["dt"] = df.apply(localizer.get_localized_dt, axis = 1)

    # subset by weekday; datetime weekday:
    #   monday = 0
    #   thursday = 3
    #   so: monday to thursday: weekday <= 3
    df = df[df["dt"].apply(lambda dt: dt.weekday() <= 3)]

    # subset by hour; datetime hour:
    #   12am: 0
    #   8pm: 9
    #   so: 8pm-11:59:59.99... pm: hour >= 9
    df = df[df["dt"].apply(lambda dt: dt.hour >= 9)]

    df = df.reset_index(drop = True)
    dbscan_debug_info["n_home_period_tweets"] = len(df)

    if (len(df) == 0):
        return merge_dicts(dbscan_debug_info, {
            "home_cluster_id": -1,
            "home_cluster_reason": "no tweets in desired time range",
            "home_cluster_centroid_lon": None,
            "home_cluster_centroid_lat": None,
            "home_cluster_count": None
        })
    else:
        dbscan_debug_info["clustering_attempted"] = 1

    ## DBSCAN ##################################################################

    print_verbose("Starting DBSCAN*")

    # convert pandas DataFrame to turicreate SFrame so we can use turicreate's
    # dbscan
    sf = turicreate.SFrame(df[[LON_COLUMN, LAT_COLUMN]])

    # DBSCAN
    dbscan_model = turicreate.dbscan.create(
        sf, radius = EPS, min_core_neighbors = MIN_POINTS, verbose = VERBOSE,
        distance = "euclidean"
    )

    # convert results to a dataframe for processing
    results = dbscan_model["cluster_id"].to_dataframe()

    # store information for dbscan type counts
    cluster_type_counts = dict(results["type"].value_counts())
    for cluster_type in DBSCAN_CLUSTER_TYPES:
        count = 0
        if (cluster_type in cluster_type_counts):
            count = cluster_type_counts[cluster_type]
        dbscan_debug_info["n_dbscan_{}_tweets".format(cluster_type)] = count

    if (pandas.isnull(results["cluster_id"]).all()):
        return merge_dicts(dbscan_debug_info, {
            "home_cluster_id": -1,
            "home_cluster_reason": "no clusters",
            "home_cluster_centroid_lon": None,
            "home_cluster_centroid_lat": None,
            "home_cluster_count": None
        })

    # create new DataFrame df_clusters by merging cluster results with the
    # tweets DataFrame. this is the same as the tweets dataframe, but with
    # clustering results appended.
    df_clusters = pandas.merge(
        df, results, left_index = True, right_on = "row_id"
    )

    # save clustering results at this point, before non-core points are cropped
    print_verbose("Creating snapshot of cluster IDs")
    create_output_df(df_clusters).to_csv(
        "%s/%s.csv" % (CLUSTERS_OUTPUT, user_id), index = False
    )

    # DBSCAN*: Only include core points (no noise or boundary points)
    df_clusters = df_clusters[df_clusters["type"] == "core"]
    df_clusters["cluster_id"] = df_clusters["cluster_id"].astype(int)

    ## CLUSTER AGGREGATE MEASURES ##############################################

    print_verbose("Creating cluster aggregate measures")
    cluster_aggregates = create_cluster_aggregates(df_clusters)
    cluster_aggregates.to_csv(
        "%s/%s.csv" % (AGGREGATES_OUTPUT, user_id), index = False
    )

    ## DETERMINATION OF HOME CLUSTER ###########################################

    print_verbose("Determining home cluster")
    (home_cluster_id, reason) = determine_home_cluster(cluster_aggregates)
    home_decision = {
        "home_cluster_id": home_cluster_id,
        "home_cluster_reason": reason,
        "home_cluster_centroid_lon": None,
        "home_cluster_centroid_lat": None,
        "home_cluster_count": None
    }

    if (home_cluster_id is not None):
        home_cluster = cluster_aggregates[
            cluster_aggregates["cluster_id"] == home_cluster_id
        ].iloc[0]
        home_decision["home_cluster_centroid_lon"] = home_cluster["centroid_lon"]
        home_decision["home_cluster_centroid_lat"] = home_cluster["centroid_lat"]
        home_decision["home_cluster_count"] = int(home_cluster["count"])

    return merge_dicts(dbscan_debug_info, home_decision)
