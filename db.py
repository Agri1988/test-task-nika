import os
from typing import List, Tuple, Iterable

import mysql.connector

config = {
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.environ.get("DB_HOST"),
    "port": os.environ.get("DB_PORT"),
    "database": os.environ.get("DB_NAME"),
    "raise_on_warnings": True,
}


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
    except mysql.connector.Error as err:
        print(f"Something went wrong. Error: {err}")
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
    # create_tables()
    insert_medical_codes(
        (
            ("qwe", "asd", "zxc", "rty"),
            ("qwe", "asd", "tyu", "ghj"),
        )
    )
