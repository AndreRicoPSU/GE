import sqlite3
from sqlite3 import Error
from pathlib import Path
import os


def create_db():
    v_data_dir = Path(__file__).parent
    v_db_GE = v_data_dir / "ge.db"

    if os.path.exists(v_db_GE):
        print("Database ge.db already exist")
        return ()

    conn = sqlite3.connect(v_db_GE)
    print("Create database successfully")

    conn.execute(
        """
        CREATE TABLE CONNECTIONS
            (ID INT      NOT NULL,
            ABBREVIATION       VARCHAR(20)     NOT NULL,         
            DATABASE           VARCHAR(60)    NOT NULL,
            CATEGORY           VARCHAR(60),
            DATASET            VARCHAR(60),
            SOURCE_PATH        VARCHAR(120),
            DESTINATION_PATH   VARCHAR(120),
            FILE_FORMAT        CHAR(5),
            FILE_NAME          VARCHAR(30),
            VERSION            CHAR(50),
            UPD_FILE           BLOB,
            SIZE               INT,
            LAST_UPDATE       INT     
            );"""
    )
    print("Table CONNECTIONS created successfully")

    conn.execute(
        """
        CREATE TABLE LOGS
            (DATE INT NOT NULL,
            ID INT      NOT NULL,
            ABBREVIATION       VARCHAR(20)     NOT NULL,         
            DATABASE           VARCHAR(60)    NOT NULL,
            CATEGORY           VARCHAR(60),
            DATASET            VARCHAR(60),
            FILE_NAME          VARCHAR(30),
            VERSION            CHAR(50),
            UPD_FILE           BLOB,
            SIZE               INT  
            );"""
    )
    print("Table LOGS created successfully")

    conn.execute(
        """
        CREATE TABLE USERS
            (USER  VARCHAR(10) NOT NULL, 
            EMAIL VARCHAR(40) NOT NULL,
            STATUS BLOB
            );"""
    )
    print("Table USERS created successfully")

    conn.close()


def drop_db():
    v_data_dir = Path(__file__).parent
    v_db_GE = v_data_dir / "ge.db"
    os.remove(v_db_GE)


if __name__ == "__main__":

    create_db()

    # drop_db()
