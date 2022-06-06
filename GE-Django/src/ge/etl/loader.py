import sqlite3
from sre_parse import expand_template
import time, sys, os
import pandas as pd
import argparse
from pathlib import Path
from datetime import datetime

"""
Sintaxe
Download/Upload + Table + Path File + Method
"""
# ts = int(time.time())
# print("0. UNIC TimeStamp: ", ts)
# print("1. Read Data: ", datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S"))


def dbUpload(table, file, method):

    if os.path.exists(Attr["db_path"]):
        conn = sqlite3.connect(Attr["db_path"])
        print("Opened database successfully")
    else:
        print("Database does not exist")
        sys.exit()

    try:
        df = pd.read_csv(file)
    except IOError as e:
        print(e)
        sys.exit(2)

    if method == "a":
        meth = "append"
    elif method == "r":
        meth = "replace"
    else:
        print("ERRO: Method is not validad")
        sys.exit(2)

    print(df)
    df.to_sql(table, conn, if_exists=meth, index=False)
    # print("upload in releasing. data was not upload")
    # https://blog.alexparunov.com/upserting-update-and-insert-with-pandas
    conn.commit()
    conn.close()
    """
    plan B:
    try:
        conn.execute(v_sql, df)
        conn.commit()
    except sqlite3.Error as er:
        print("SQLite error: %s" % (" ".join(er.args)))
        print("Exception class is: ", er.__class__)
    """


def dbDownload(table, file):
    if os.path.exists(Attr["db_path"]):
        conn = sqlite3.connect(Attr["db_path"])
        print("Opened database successfully")
    else:
        print("Database does not exist")
        sys.exit()

    v_query = """SELECT * FROM {}""".format(table)
    dfd = pd.read_sql_query(v_query, conn)
    dfd.to_csv(file)
    print(f"File created with success")
    conn.close()
    print("Database closed")


def dbShow(table):
    if os.path.exists(Attr["db_path"]):
        conn = sqlite3.connect(Attr["db_path"])
        print("Opened database successfully")
    else:
        print("Database does not exist")
        sys.exit()

    v_query = """SELECT * FROM {}""".format(table)
    dfd = pd.read_sql_query(v_query, conn)
    print(dfd)
    conn.close()


if __name__ == "__main__":

    Attr = {}
    v_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.txt")
    print(v_path)
    with open(v_path) as f:
        for line in f:
            (k, v) = line.split()
            Attr[k] = v

    list_tables = [
        "CONNECTORS",
        "DATABASES",
        "DATASETS",
        "CATEGORIES",
        "GROUPS",
        "KEYS",
        "KEYWORDS",
        "KEYLINKS",
        "WORDMAP",
        "BLACKLIST",
    ]

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-l",
        "--list_tables",
        help="list all available tables to upload and download",
        action="store_true",
    )
    parser.add_argument(
        "-u", "--upload", action="store_true", help="Upload data to GE.db"
    )
    parser.add_argument(
        "-s", "--show", action="store_true", help="show data from GE.db"
    )
    parser.add_argument(
        "-d", "--download", action="store_true", help="Download data to GE.db"
    )
    parser.add_argument(
        "-t",
        "--table",
        type=str,
        metavar="table",
        action="store",
        default=None,
        help="Name of the table where the data will be uploaded or downloaded. Use -l to list available tables",
    )
    parser.add_argument(
        "-f",
        "--file",
        type=str,
        metavar="file",
        action="store",
        default=None,
        help="A directory to use for input and output files",
    )
    parser.add_argument(
        "-m",
        "--method",
        type=str,
        choices=["a", "r"],
        action="store",
        default="r",
        help="Method to update data in target tables via Append or Replace",
    )

    # if no arguments, print usage and exit
    if len(sys.argv) < 2:
        parser.print_usage()
        # sys.exit(2)

    args = parser.parse_args()

    # Check Debug
    args.upload = True
    args.table = "Blacklist"
    args.file = "/Users/andrerico/hall/igem/ge/database/load_files/blacklist.csv"

    check_table = False
    check_file = False

    if args.list_tables:
        print("Tables availables to updates: \n", list_tables)

    # check if the table exists
    if args.table != None:
        v_table = args.table.upper()
        if v_table in list_tables:
            check_table = True
        else:
            print(f"ERRO: Table is not available")
            sys.exit(1)

    # check if the file exists
    if args.file != None:
        if not os.path.exists(args.file):
            if args.upload:
                print(f"ERROR: File is not available")
                sys.exit(1)
            else:
                print(f"File will be created")
        else:
            check_file = True

    if args.upload:
        if check_table and check_file:
            dbUpload(args.table, args.file, args.method)
        else:
            print(f"ERRO: Mandatory to inform the parameters for table and file path")

    if args.download:
        if check_table:
            dbDownload(v_table, args.file)
        else:
            ("some erro")
        # print("Under Devolopment")

    if args.show:
        if check_table:
            dbShow(v_table)
        else:
            ("some erro")
        # print("Under Devolopment")
