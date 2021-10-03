# -*- coding: utf-8 -*-
"""Code to create and fill `TDT4225ProjectGroup78` database with data.

This module contains code that creates the `TDT4225ProjectGroup78` database,
creates the tables of the `TDT4225ProjectGroup78` database, and fills the
database with data. Also provides an interface to queries performed on the
database.

"""
import os
import sys
import pandas as pd
import numpy as np
import time
from mysql import connector
from mysql.connector import errorcode
from sqlalchemy import create_engine
import pymysql
import queries

def create_database(cursor, DB_NAME):
    """Helper function to create database.

    Tries to create the database, prints and error and exists if unsuccessful.

    Parameters
    ----------
    cursor : :obj:
        The mysql.connector.cursor object used to execute MySQL queries.
    DB_NAME : str
        The MySQL database name (`TDT4225ProjectGroup78`)

    """
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME)
        )
    except connector.Error as err:
        print("Failed creating database: {}".format(err))
        sys.exit(1)


def setup_database(user, password, DB_NAME, TABLES):
    """Function to setup database and tables.

    Drops the `TDT4225ProjectGroup78` database if already exists, then creates
    the database and executes the table initialization statements.

    Parameters
    ----------
    user : str
        The entered MySQL user.
    password : str
        The entered MySQL password.
    DB_NAME : str
        The MySQL database name (`TDT4225ProjectGroup78`).
    TABLES : dict
        A dict containing the tables and their MySQL statements. Used to set up
        the database with the correct tables.

    """

    # Instantiate connection
    with connector.connect(user=user, password=password) as cnx:

        # Instantiate cursor object
        with cnx.cursor() as cursor:
            # Start by dropping database
            try:
                cursor.execute("DROP DATABASE {}".format(DB_NAME))
            except connector.Error as err:
                if err.errno == errorcode.ER_BAD_DB_ERROR:
                    print("Database {} does not exists.".format(DB_NAME))
                else:
                    print(err.msg)
                    sys.exit(1)
            # Create database
            finally:
                create_database(cursor, DB_NAME)
                print("Database {} created successfully.".format(DB_NAME))
                # Set database tame
                cnx.database = DB_NAME

            # Create Tables if not exist
            for table_name in TABLES:
                table_description = TABLES[table_name]
                try:
                    print("Creating table {}: ".format(table_name), end="")
                    cursor.execute(table_description)
                except connector.Error as err:
                    if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                        print("already exists.")
                    else:
                        print(err.msg)
                else:
                    print("OK")


def parse_data():
    """Parse data from `.plt` files into Pandas DataFrames.

    Returns
    -------
    user_df : Pandas DataFrame
        Table of user information.
    activity_df : Pandas DataFrame
        Table of activity information.
    trackpoint_df : Pandas DataFrame
        Table of trackpoint information.
    """
    # Load user data into Pandas DataFrame
    user_ids = sorted(os.listdir("../dataset/Data/"))
    # Find labeled users
    with open("../dataset/labeled_ids.txt", "r") as f:
        labeled_users = f.read().splitlines()
    has_labels = [True if uid in labeled_users else False for uid in user_ids]
    user_df = pd.DataFrame({"id": user_ids, "has_labels": has_labels})

    # Activity and Trackpoint columns
    activity_cols = [
        "id",
        "user_id",
        "transportation_mode",
        "start_date_time",
        "end_date_time",
    ]
    trackpoint_cols = [
        "lat",
        "lon",
        "ignore",
        "altitude",
        "date_days",
        "date",
        "time"
    ]

    # lists to store dataframes
    trackpoint_ll = []
    activity_ll = []

    aid = 0
    for uid in user_ids:
        user_path = f"../dataset/Data/{uid}/"
        trajectory_path = user_path + "Trajectory/"

        # Load labels if they exist
        labels = []
        if os.path.exists(user_path + "labels.txt"):
            labels = pd.read_csv(user_path + "labels.txt", sep="\t")
            labels["Start Time"] = pd.to_datetime(labels["Start Time"])
            labels["End Time"] = pd.to_datetime(labels["End Time"])

        for filename in os.listdir(trajectory_path):
            # Load trackpoints
            df = pd.read_csv(
                trajectory_path + filename, skiprows=6, names=trackpoint_cols
            )
            # Ignore if more than 2500 records
            if len(df) > 2500:
                continue
            # Convert to datetime
            df["date_time"] = pd.to_datetime(df["date"] + " " + df["time"])
            df = df.drop(columns=["date", "time", "ignore"])

            # Create activity record
            activity = {}
            activity["id"] = aid
            activity["user_id"] = uid
            activity["start_date_time"] = df["date_time"].iloc[0]
            activity["end_date_time"] = df["date_time"].iloc[-1]
            activity["transportation_mode"] = np.nan

            # Find transportation mode
            # Makes sure that duplicate labels are handled by adding additional
            # Activities
            if len(labels) > 0:
                # Find labels that matches the current trackpoint start and
                # end time
                temp_df = labels.loc[
                    (labels["Start Time"] == activity["start_date_time"])
                    & (labels["End Time"] == activity["end_date_time"])
                ]
                # If empty, add current activity to list
                if len(temp_df) == 0:
                    # Add aid to trackpoint
                    df["activity_id"] = aid

                    trackpoint_ll.append(df)
                    activity_ll.append(pd.Series(activity))

                    # increment aid
                    aid += 1
                # Else, loop through entries in the labels and add new
                # activities for each match
                else:
                    for tm in temp_df["Transportation Mode"].values:
                        # Add aid to trackpoint
                        df["activity_id"] = aid

                        # Create new activity
                        activity = {}
                        activity["id"] = aid
                        activity["user_id"] = uid
                        activity["start_date_time"] = df["date_time"].iloc[0]
                        activity["end_date_time"] = df["date_time"].iloc[-1]
                        activity["transportation_mode"] = tm

                        trackpoint_ll.append(df)
                        activity_ll.append(pd.Series(activity))

                        # increment aid
                        aid += 1
            # If there's no match, add current activity
            else:
                # Add aid to trackpoint
                df["activity_id"] = aid
                trackpoint_ll.append(df)

                activity_ll.append(pd.Series(activity))
                # increment aid
                aid += 1

    # Create dataframes from saved lists
    trackpoint_df = pd.concat(trackpoint_ll).reset_index(drop=True)
    trackpoint_df["id"] = [i for i in range(len(trackpoint_df))]
    activity_df = pd.DataFrame().append(activity_ll)

    # Replace -777 as it is an invalid altitude
    trackpoint_df["altitude"].replace(-777, np.nan, inplace=True)
    return (user_df, activity_df, trackpoint_df)


def insert_data(user, password, DB_NAME):
    """Insert data into MySQL database.

    Parameters
    ----------
    user : str
        The entered MySQL user
    password : str
        The entered MySQL password
    DB_NAME : str
        The MySQL database name (`TDT4225ProjectGroup78`)

    """

    start_time = time.time()
    user_df, activity_df, trackpoint_df = parse_data()
    print(f"Data parsed successfully. Time taken: {time.time() - start_time:.2f} seconds")

    # Instantiate connection
    with create_engine(
        f"mysql+pymysql://{user}:{password}@localhost/{DB_NAME}"
    ).connect() as cnx:
        user_table = "User"
        activity_table = "Activity"
        trackpoint_table = "TrackPoint"

        start_time = time.time()
        try:
            user_df.to_sql(user_table, cnx, if_exists="append", index=False)
        except Exception as ex:
            print(ex)
        else:
            print(
                f"Table {user_table} created successfully. Time taken: {time.time() - start_time:.2f} seconds"
            )

        start_time = time.time()
        try:
            activity_df.to_sql(activity_table, cnx, if_exists="append", index=False)
        except Exception as ex:
            print(ex)
        else:
            print(
                f"Table {activity_table} created successfully. Time taken: {time.time() - start_time:.2f} seconds"
            )

        start_time = time.time()
        try:
            trackpoint_df.to_sql(trackpoint_table, cnx, if_exists="append", index=False)
        except Exception as ex:
            print(ex)
        else:
            print(
                f"Table {trackpoint_table} created successfully. Time taken: {time.time() - start_time:.2f} seconds"
            )


def query_database(user, password, DB_NAME):
    """Call the different query functions.

    Parameters
    ----------
    user : str
        The entered MySQL user
    password : str
        The entered MySQL password
    DB_NAME : str
        The MySQL database name (`TDT4225ProjectGroup78`)
    """

    # Instantiate connection
    with create_engine(
        f"mysql+pymysql://{user}:{password}@localhost/{DB_NAME}"
    ).connect() as cnx:

        # Query 1
        print("Query 1:")
        queries.query_1(cnx)

        # Query 2
        print("Query 2:")
        queries.query_2(cnx)

        # Query 3
        print("Query 3:")
        queries.query_3(cnx)

        # Query 4
        print("Query 4:")
        queries.query_4(cnx)

        # Query 5
        print("Query 5:")
        queries.query_5(cnx)

        # Query 6
        print("Query 6:")
        queries.query_6(cnx)

        # Query 7
        print("Query 7:")
        queries.query_7(cnx)

        # Query 8
        print("Query 8:")
        queries.query_8(cnx)

        # Query 9
        print("Query 9:")
        queries.query_9(cnx)

        # Query 10
        print("Query 10:")
        queries.query_10(cnx)

        # Query 11
        print("Query 11:")
        queries.query_11(cnx)

        # Query 12
        print("Query 12")
        queries.query_12(cnx)
