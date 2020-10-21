""" Import required columns into a user.id-indexed SQLite3 database, for fast
user.id queries. """

from __future__ import annotations

import csv
import glob
import gzip
import os
import sqlite3
import typing

import tqdm

from . import dbscan
from . import measures

REQUIRED_COLUMNS = list(
    dbscan.REQUIRED_COLUMNS.union(measures.REQUIRED_COLUMNS)
)

# TODO: generate this automatically
SQL_TABLE_NAME = "geotweets"
SQL_COLUMNS = [column.replace(".", "_") for column in REQUIRED_COLUMNS]
SQL_INIT = """
    CREATE TABLE IF NOT EXISTS geotweets(
        created_at TEXT,
        user_name TEXT,
        user_id INTEGER,
        coordinates_coordinates_0 REAL,
        coordinates_coordinates_1 REAL
    );
    CREATE TABLE IF NOT EXISTS files(
        filename TEXT
    );
"""

# obtained from https://en.wikipedia.org/wiki/List_of_file_signatures
COMPRESSED_ARCHIVE_SIGNATURES = {
    b"\x42\x5a\x68": "bz2",
    b"\x1f\x8b": "gzip",
    b"\xfd\x37\x7a\58\x5a\x00": "xz"
}
MAX_SIGNATURE_LENGTH = max(
    len(signature)
    for signature in COMPRESSED_ARCHIVE_SIGNATURES
)

class MultiCursor():
    """ The MultiCursor object roughly mimics important parts of the
    sqlite3.Cursor API and generalizes it to be distributed to multiple
    sqlite3.Connection objects. See MultiSqlite for more info.

    Attributes:
        cursors: A generator yielding cursors for the connected databases.
        description: The description of the first cursor, initialized after
            running self.execute().
        results: A self reference to this class, initialized after running
            self.execute().
    """
    def __init__(self, connections: typing.List[sqlite3.Connection]):
        """ Initializes Multicursor object.

        Args:
            connections: A list of sqlite3.Connection objects.
        """

        if len(connections) == 0:
            raise Exception("no connections passed to MultiCursor instance")

        self.cursors = [
            connection.cursor()
            for connection in connections
        ]
        self.results = None

    def __iter__(self) -> typing.Iterable:
        """ Iterate over results of all connected cursors. """

        return self.results

    @property
    def description(self):
        return self.cursors[0].description

    def close(self) -> None:
        """ Close all cursors. """

        for cursor in self.cursors:
            cursor.close()

    def fetchall(self) -> typing.List[tuple]:
        """ Return all results as a list. """

        return list(iter(self))

    def execute(self, query: str,
                values: typing.Union[tuple, None] = None) -> MultiCursor:
        """ Run a query on all connected cursors.

        Args:
            query: The SQL query to run.
            values: The values to supply to the query.

        Returns:
            This MultiCursor.
        """

        for cursor in self.cursors:
            if values is None:
                cursor.execute(query)
            else:
                cursor.execute(query, values)

        self.results = (
            result
            for cursor in self.cursors
            for result in cursor
        )
        return self

class MultiSqlite():
    """ The MultiSqlite object roughly mimics important parts of the
    sqlite3.Connection API and generalizes it to access multiple different
    databases at the same time - queries are distributed to the connected
    databases via the MultiCursor object. The primary purpose of this class is
    to make multiple databases queryable via pandas.read_sql_query.

    Note that all database operations are localized to each connected database
    rather than performed on the union of all available data.

    Attributes:
        db_paths: A list of connected databases.
        connections: A list of open connections, if any.
    """

    def __init__(self, db_paths: typing.Union[str, list]):
        """ Initialize a MultiSqlite object.

        Args:
            db_paths: Either a list of paths or a glob pattern pointing to the
                databases that should be connected.
        """

        if isinstance(db_paths, list):
            self.db_paths = db_paths
        elif isinstance(db_paths, str):
            self.db_paths = glob.glob(db_paths)
        else:
            raise ValueError

        self.connections = []
        self.connect_all()

    def __repr__(self) -> str:
        return "{}(db_paths=[{}])".format(
            self.__class__.__name__, ",".join(self.db_paths)
        )

    def __enter__(self) -> MultiSqlite:
        self.connect_all()
        return self

    def __exit__(self, type_, value, traceback) -> None:
        self.close_all()

    def connect_all(self) -> None:
        """ Connect to all linked databases. """

        self.connections = [
            sqlite3.connect(path)
            for path in self.db_paths
        ]

    def close_all(self, commit: bool = True) -> None:
        """ Close all open database conenctions, optionally committing before
        closing.

        Args:
            commit: Toggles whether or not to commit changes before closing.
        """

        for connection in self.connections:
            if commit:
                connection.commit()
            connection.close()
        self.connections = []

    def cursor(self) -> MultiCursor:
        """ Create a new MultiCursor object from this MultiSqlite's
        connected databases. """

        return MultiCursor(self.connections)

    def execute(self, *cursor_args, **cursor_kwargs) -> MultiCursor:
        """ Run a query on all connected databases.

        Args:
            cursor_args, cursor_kwargs: Passed through to MultiCursor.execute.

        Returns:
            A MultiCursor object.
        """

        cursor = MultiCursor(self.connections)
        cursor.execute(*cursor_args, **cursor_kwargs)
        return cursor

def detect_compression(file: str) -> typing.Union[str, None]:
    """ Detect the compression of a file, if any.

    Adapted from https://stackoverflow.com/a/13044946

    Args:
        file: The file to check.
    Returns:
        The compression level as a string, if any, or None.
    """

    with open(file, "rb") as input_fp:
        head = input_fp.read(MAX_SIGNATURE_LENGTH)

    for signature, compression in COMPRESSED_ARCHIVE_SIGNATURES.items():
        if head.startswith(signature):
            return compression
    return None

def init_db(paths: typing.Union[str, typing.List[str]],
            db_path: str) -> None:
    """ Import CSV files generated by geotweets-utils into an SQLite database.

    Args:
        paths: A single path or list of paths to CSV files to import.
        db_path: The database that the data should be imported into.
        skip_ids: A set containing the user IDs that should be skipped, if any.
    """
    #pylint: disable=invalid-name

    db = sqlite3.connect(db_path)
    db.executescript(SQL_INIT)

    paths_to_import = [
        path
        for path in paths
        if next(db.execute(
            "SELECT COUNT(*) FROM files WHERE filename = ?",
            (os.path.basename(path),)
        ))[0] == 0
    ]

    if len(paths_to_import) == 0:
        print("nothing to import")
        db.close()
        return

    with db:
        print("dropping existing index, if any")
        db.execute("DROP INDEX IF EXISTS idx_user_id")

    for path in tqdm.tqdm(paths_to_import, desc="importing files", position=0):
        filename = os.path.basename(path)

        if detect_compression(path) == "gzip":
            input_fp = gzip.open(path, "rt")
        else:
            input_fp = open(path, "r")

        # for some reason some rows have null bytes
        reader = csv.DictReader(
            row.replace("\0", "")
            for row in input_fp
        )

        with db:
            db.executemany(
                "INSERT INTO {table_name}({columns}) VALUES ({placeholders})".format(
                    table_name=SQL_TABLE_NAME,
                    columns=", ".join(column for column in SQL_COLUMNS),
                    placeholders=", ".join("?" for column in SQL_COLUMNS)
                ),
                (
                    tuple([row[column] for column in REQUIRED_COLUMNS])
                    for row in tqdm.tqdm(
                        reader,
                        desc="importing {}".format(filename),
                        position=1,
                        leave=None
                    )
                )
            )
            db.execute("INSERT INTO files(filename) VALUES (?)", (filename,))
            input_fp.close()

    with db:
        print("creating new index")
        db.execute("CREATE INDEX idx_user_id ON geotweets(user_id)")

    db.close()
