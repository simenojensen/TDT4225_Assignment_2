# -*- coding: utf-8 -*-
"""MySQL `TDT4225ProjectGroup78` database statements

This module contains code which defines the tables, their fields and their
constraints used to setup the `TDT4225ProjectGroup78` database. The database
name is stored in the string `DB_NAME` variable. The tables are stored in a dict
named `TABLES`.

"""

# Name of database
DB_NAME = "TDT4225ProjectGroup78"


# dict of the MySQL tables, their fields and their constraints.
TABLES = {}

# UserName was assumed to not necessarily be unique.
TABLES["User"] = (
    "CREATE TABLE `User` ("
    "  `id` varchar(3) NOT NULL,"
    "  `has_labels` boolean NOT NULL,"
    "  CONSTRAINT `User_PK` PRIMARY KEY (`id`)"
    ") ENGINE=InnoDB"
)

TABLES["Activity"] = (
    "CREATE TABLE `Activity` ("
    "  `id` int NOT NULL,"
    "  `user_id` varchar(3) NOT NULL,"
    "  `transportation_mode` varchar(20),"
    "  `start_date_time` datetime NOT NULL,"
    "  `end_date_time` datetime NOT NULL,"
    "  CONSTRAINT `Activity_PK` PRIMARY KEY (`id`),"
    "  CONSTRAINT `Activity_FK` FOREIGN KEY (`user_id`) REFERENCES `User` (`id`)"
    "          ON UPDATE CASCADE ON DELETE CASCADE"
    ") ENGINE=InnoDB"
)

TABLES["TrackPoint"] = (
    "CREATE TABLE `TrackPoint` ("
    "  `id` int NOT NULL,"
    "  `activity_id` int NOT NULL,"
    "  `lat` double NOT NULL,"
    "  `lon` double NOT NULL,"
    "  `altitude` int,"
    "  `date_days` double NOT NULL,"
    "  `date_time` datetime NOT NULL,"
    "  CONSTRAINT `TrackPoint_PK` PRIMARY KEY (`id`),"
    "  CONSTRAINT `TrackPoint_FK` FOREIGN KEY (`activity_id`) REFERENCES `Activity` (`id`)"
    "          ON UPDATE CASCADE ON DELETE CASCADE"
    ") ENGINE=InnoDB"
)
