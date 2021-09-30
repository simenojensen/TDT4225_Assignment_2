# -*- coding: utf-8 -*-
"""
TDT4225 Very Large, Distributed Data Volumes - Assignment 2
Author: Simen Omholt-Jensen

This module contains code that runs the strava interface.
"""
import sys
import getpass

import tables
import database

from tables import TABLES
from tables import DB_NAME
from database import setup_database
from database import insert_data
from database import query_database

def main():
    """Sets up the database and runs the program.

    Program prompts user for their MySQL login information. A database called
    `TDT4225ProjectGroup78` is created and filled with data from the `.plt`
    files in the `dataset` folder. The program then queries the database to
    answer the questions found in the assignment text.

    """

    # Prompt the user for their MySQL login inforamtion
    user = input("Enter MySQL user: ")
    password = getpass.getpass(prompt="Enter MySQL password: ")

    # create strava database
    setup_database(user, password, DB_NAME, TABLES)
    insert_data(user, password, DB_NAME)

    # Perform queries
    query_database(user, password, DB_NAME)


if __name__ == "__main__":
    main()
