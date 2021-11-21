import os
import time
from typing import Iterable

import mysql.connector
from mysql.connector import DatabaseError

config = {
    "user": os.environ.get("MYSQL_USER"),
    "password": os.environ.get("MYSQL_PASSWORD"),
    "host": os.environ.get("MYSQL_HOST"),
    "port": os.environ.get("MYSQL_PORT"),
    "database": os.environ.get("MYSQL_DATABASE"),
    "raise_on_warnings": True,
}


def check_db_connection():
    while True:
        try:
            print("Check DB connection", flush=True)
            cnx = mysql.connector.connect(**config)
            cnx.ping()
            return True
        except DatabaseError:
            print(f"Connection failed. Sleep 5 seconds", flush=True)
            time.sleep(5)


def create_tables():
    query = """
        CREATE TABLE medical_codes (
        pk BIGINT not null primary key auto_increment,
        group_code varchar(128) not null ,
        group_desc varchar(512) not null ,
        code varchar(128) not null unique,
        code_desc varchar(512) not null 
        );
    """
    try:
        cnx = mysql.connector.connect(**config)
        cursor = cnx.cursor()
        cursor.execute(query)
        print("Table created", flush=True)
    except mysql.connector.Error as err:
        print(f"Something went wrong. Error: {err}", flush=True)
    else:
        cnx.close()


def insert_medical_codes(medical_codes: Iterable):
    query = "INSERT INTO medical_codes (group_code, group_desc, code, code_desc) VALUES( %s, %s, %s, %s)"
    try:
        cnx = mysql.connector.connect(**config)
        cursor = cnx.cursor()
        cursor.executemany(query, medical_codes)
        cnx.commit()
    except mysql.connector.Error as err:
        print(f"Something went wrong. Error: {err}")
    else:
        cnx.close()


if __name__ == "__main__":
    if check_db_connection():
        create_tables()