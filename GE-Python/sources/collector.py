import requests
import os
import sys
import sqlite3
import time
import patoolib
import argparse
import pandas as pd


def db_update(v_sql, v_data):
    try:
        conn.execute(v_sql, v_data)
        conn.commit()
    except sqlite3.Error as er:
        print("SQLite error: %s" % (" ".join(er.args)))
        print("Exception class is: ", er.__class__)


def db_select(connector):

    if connector == "ALL":
        cursor = conn.execute("""SELECT * FROM CONNECTORS WHERE UPD_FILE = 1""")
    elif connector == "ALLCONNECTORS":
        cursor = conn.execute("""SELECT * FROM CONNECTORS""")
    else:
        v_query = "SELECT * FROM CONNECTORS WHERE CONNECTOR = ?"
        cursor = conn.execute(v_query, (connector,))
    return cursor


if __name__ == "__main__":

    # SQL scripts for interactions with GE-DB
    db_log = "INSERT INTO LOGS VALUES(?, ?, ?, ?, ?, ?, ?, ?)"  # LOG Table
    db_log_conn = "UPDATE CONNECTORS SET LAST_UPDATE=?, SOURCE_FILE_VERSION=?, SOURCE_FILE_SIZE=?, TARGET_FILE_SIZE=? WHERE CONNECTOR=?"  # CONNECTORS Table
    db_upd_file = "UPDATE CONNECTORS SET UPD_FILE=? WHERE CONNECTOR=?"

    # connect DB
    Attr = {}
    # v_dir = Path(__file__).parent / "/users/andrerico/hall/igem/ge/config.txt"

    v_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.txt")

    with open(str(v_dir)) as f:
        # with open("/users/andrerico/hall/igem/ge/config.txt") as f:
        for line in f:
            (k, v) = line.split()
            Attr[k] = v

    if os.path.exists(Attr["db_path"]):
        conn = sqlite3.connect(Attr["db_path"])
        print("Opened database successfully")
    else:
        print("Database does not exist")
        sys.exit()

    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--process", action="store_true", help="")
    parser.add_argument("-s", "--show", action="store_true", help="")
    parser.add_argument("-r", "--reset", action="store_true", help="")
    parser.add_argument("-a", "--active", action="store_true", help="")
    parser.add_argument("-d", "--deactive", action="store_true", help="")
    parser.add_argument(
        "-c",
        "--connector",
        type=str,
        metavar="connector",
        action="store",
        default="all",
        help="",
    )

    args = parser.parse_args()

    # debug variables
    # args.process = True
    # args.connector = "CTD_TEST_SML"

    if len(sys.argv) < 2:
        parser.print_usage()
        sys.exit(2)

    if args.show:
        v_query = """SELECT * FROM CONNECTORS"""
        dfd = pd.read_sql_query(v_query, conn)
        print(dfd)

    if args.active:
        if args.connector == "all":
            print("To activate all connectors, inform: -c allconnectors")
            sys.exit(2)
        elif args.connector != None:
            cursor = db_select(args.connector.upper())

        for row in cursor:
            db_set_act = (1, row[0])
            db_update(db_upd_file, db_set_act)
            print("Activated: ", row[0])

    if args.deactive:
        if args.connector == "all":
            print("To disdable all connectors, inform: -c allconnectors")
            sys.exit(2)
        elif args.connector != None:
            cursor = db_select(args.connector.upper())

        for row in cursor:
            db_set_act = (0, row[0])
            db_update(db_upd_file, db_set_act)
            print("Deactivated: ", row[0])

    if args.reset:
        if args.connector == "all":
            print("To reset all connectors, inform: -c allconnectors")
            sys.exit(2)
        elif args.connector != None:
            cursor = db_select(args.connector.upper())

        for row in cursor:  # Future - add msm with cursor empty
            print("Reseting: ", row[0])
            print("    DATABASE = ", row[1])
            print("    DATASET  = ", row[2])

            # Update CONNECTORS table with new version
            db_log_data = (
                "",
                "",
                "",
                "",
                row[0],
            )
            db_update(db_log_conn, db_log_data)

    if args.process:

        if args.connector != None:
            cursor = db_select(args.connector.upper())

        v_path_file = Attr["psa_path"]

        v_time = int(time.time())

        db_log = "INSERT INTO LOGS VALUES(?, ?, ?, ?, ?, ?, ?, ?)"  # LOG Table

        for row in cursor:
            print("dentro do lood do process")
            print("Starting: ", row[0])
            print("    DATABASE = ", row[1])
            print("    DATASET  = ", row[2])

            # config PSA folder (persistent staging area)
            v_dir = v_path_file + "/" + row[0]
            v_source_file = v_dir + "/" + row[7]
            v_target_file = v_dir + "/" + row[11]
            if not os.path.isdir(v_dir):
                os.makedirs(v_dir)
                print("    FOLDER  = ", v_dir)

            v_file_url = row[5]

            try:
                v_version = str(requests.get(v_file_url, stream=True).headers["etag"])
            except:
                print("erro na extracao do etag")
                v_version = "0"  # Need better treatment here. Maybe: v_size_new = str(requests.get(file_url, stream=True).headers["Content-length"])

            # New version will download file, same version only inform the log table
            if row[9] == v_version:
                print("    VERSION = ", "Same - download canceled \n")
                db_log_data = (
                    v_time,
                    row[0],
                    row[1],
                    row[2],
                    row[7],
                    v_version,
                    0,
                    row[10],
                )

                # db_update(db_log, db_log_data)

            else:
                # Donwload file from internet
                if os.path.exists(v_target_file):
                    os.remove(v_target_file)
                if os.path.exists(v_target_file):
                    os.remove(v_source_file)
                print("    VERSION = ", "download in process ")
                r = requests.get(v_file_url, stream=True)
                with open(v_source_file, "wb") as download:
                    for chunk in r.iter_content(chunk_size=1000000):
                        if chunk:
                            download.write(chunk)  # Improve Point
                # Update LOG table if new version
                v_size = str(os.stat(v_source_file).st_size)

                print(
                    "    VERSION = ",
                    "download finished / size: ",
                    v_size,
                    " / version: ",
                    v_version,
                )
                db_log_data = (
                    v_time,
                    row[0],
                    row[1],
                    row[2],
                    row[7],
                    v_version,
                    2,
                    v_size,
                )
                db_update(db_log, db_log_data)

                if row[6] == 1:  # compact file
                    patoolib.extract_archive(str(v_source_file), outdir=str(v_dir))
                    os.remove(v_source_file)

                # Update CONNECTORS table with new version
                db_log_data = (
                    int(time.time()),
                    v_version,
                    v_size,
                    str(os.stat(v_target_file).st_size),
                    row[0],
                )
                db_update(db_log_conn, db_log_data)


conn.close()

print("Operation done successfully")
