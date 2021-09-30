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

TABLES["User"] = (
    "CREATE TABLE `User` ("
    "  `id` VARCHAR(3) NOT NULL,"
    "  `has_labels` BOOLEAN NOT NULL,"
    "  CONSTRAINT `User_PK` PRIMARY KEY (`id`)"
    ") ENGINE=InnoDB"
)

TABLES["Activity"] = (
    "CREATE TABLE `Activity` ("
    "  `id` INT NOT NULL,"
    "  `user_id` VARCHAR(3) NOT NULL,"
    "  `transportation_mode` VARCHAR(20),"
    "  `start_date_time` DATETIME NOT NULL,"
    "  `end_date_time` DATETIME NOT NULL,"
    "  CONSTRAINT `Activity_PK` PRIMARY KEY (`id`),"
    "  CONSTRAINT `Activity_FK` FOREIGN KEY (`user_id`) REFERENCES `User` (`id`)"
    "          ON UPDATE CASCADE ON DELETE CASCADE"
    ") ENGINE=InnoDB"
)

TABLES["TrackPoint"] = (
    "CREATE TABLE `TrackPoint` ("
    "  `id` INT NOT NULL,"
    "  `activity_id` INT NOT NULL,"
    "  `lat` DOUBLE NOT NULL,"
    "  `lon` DOUBLE NOT NULL,"
    "  `altitude` INT,"
    "  `date_days` DOUBLE NOT NULL,"
    "  `date_time` DATETIME NOT NULL,"
    "  CONSTRAINT `TrackPoint_PK` PRIMARY KEY (`id`),"
    "  CONSTRAINT `TrackPoint_FK` FOREIGN KEY (`activity_id`) REFERENCES `Activity` (`id`)"
    "          ON UPDATE CASCADE ON DELETE CASCADE"
    ") ENGINE=InnoDB"
)
