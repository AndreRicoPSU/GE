import sys
import os
import sqlite3
import time
import math
import argparse
import pandas as pd
import re
import concurrent.futures
from itertools import combinations
from itertools import islice

# IMPORT: I have problem to run on python > 3.9 with global variant does not in Concurrent.future


def vr_config(dir):
    attr = {}
    with open(str(dir)) as f:
        for line in f:
            (k, v) = line.split()
            attr[k] = v
    return attr


def chunked_iterable(iterable, size):
    while True:
        chunk = list(islice(iterable, size))
        if not chunk:
            break
        yield chunk


def mapper(lines):
    global v_blacklist
    WORD_RE = re.compile(r"[\w']+")
    # https://docs.python.org/3/library/re.html#re.compile / # https://regex101.com/r/CMGOHz/1
    # WORD_RE = re.compile(r"\b\d*[^\W\d_][^\W_]*\b")
    df_mapper = pd.DataFrame(columns=["word1", "word2", "count"])
    tmp = []
    for line in lines:
        # Preparation before mapping
        words = WORD_RE.findall(line)
        words = [x.lower() for x in words]
        words.sort()
        words = list(set(words))  # Point for contestation
        words.sort()
        words = list(filter(lambda w: w not in v_blacklist, words))
        # Mapping
        for (x, y) in combinations(words, 2):
            # if x != y: -->> works opposite with list(set(words))
            if x < y:
                tmp.append([x, y, 1])
            else:
                tmp.append([y, x, 1])
    df_mapper = pd.DataFrame(tmp, columns=["word1", "word2", "count"])
    return df_mapper


def ingestor(DF):
    # Read from KEYS table to add attributes in keylinks
    DFKS = pd.read_sql_query("""SELECT * FROM GE_KEYGE""", conn)
    DFKS.sort_values(by="keyge", inplace=True)
    DFKS.set_index("keyge")

    DF["GRP1N"] = DF.set_index("KEY1").index.map(
        DFKS.set_index("KEY")["GROUP"])
    DF["CAT1N"] = DF.set_index("KEY1").index.map(
        DFKS.set_index("KEY")["CATEGORY"])
    DF["GRP2N"] = DF.set_index("KEY2").index.map(
        DFKS.set_index("KEY")["GROUP"])
    DF["CAT2N"] = DF.set_index("KEY2").index.map(
        DFKS.set_index("KEY")["CATEGORY"])
    DF["CKEY"] = DF["KEY1"] + str("-") + DF["KEY2"]
    DF["CGRP"] = DF["GRP1N"] + str("-") + DF["GRP2N"]
    DF["CCAT"] = DF["CAT2N"] + str("-") + DF["CAT2N"]
    columnsTitles = [
        "CKEY",
        "DATABASE",
        "DATASET",
        "CGRP",
        "CCAT",
        "KEY1",
        "KEY2",
        "COUNT",
    ]
    DF.reindex(columns=columnsTitles)
    DF = DF.groupby(
        ["CKEY", "DATABASE", "DATASET", "CGRP", "CCAT", "KEY1", "KEY2"], as_index=False
    )["COUNT"].sum()
    return DF


def db_open(path):
    if os.path.exists(path):
        conn = sqlite3.connect(path)
        print("STATUS: GE-db opened successfully")
    else:
        print("WARNING: database does not exist")
        sys.exit()
    return conn


def db_select_conn(table, where):
    if where == "ALL":
        query = """SELECT * FROM {}""".format(
            table,
        )
    else:
        query = """SELECT * FROM {} WHERE {}""".format(
            table,
            where,
        )
    try:
        cursor = conn.execute(query)
    except sqlite3.Error as er:
        print("SQLite error: %s" % (" ".join(er.args)))
        print("Exception class is: ", er.__class__)
    return cursor


def db_select_df(table, where):
    if where == "ALL":
        query = """SELECT * FROM {}""".format(
            table,
        )
    else:
        query = """SELECT * FROM {} WHERE {}""".format(
            table,
            where,
        )
    try:
        return pd.read_sql_query(query, conn)
    except sqlite3.Error as er:
        print("SQLite error: %s" % (" ".join(er.args)))
        print("Exception class is: ", er.__class__)


def db_delete(table, where):
    if where == "ALL":
        query = """DELETE FROM {}""".format(
            table,
        )
    else:
        query = """DELETE from {} where {}""".format(
            table,
            where,
        )
    try:
        conn.execute(query)
        conn.commit()
    except sqlite3.Error as er:
        print("SQLite error: %s" % (" ".join(er.args)))
        print("Exception class is: ", er.__class__)


def db_masterdata(conn):

    # DFWK = pd.read_sql_query("""SELECT * FROM GE_KEYWORD""", conn)
    DFWK = pd.read_sql_query(
        """SELECT GE_KEYWORD.word, ge_keyge.keyge, ge_keyge.id FROM GE_KEYWORD LEFT JOIN GE_KEYGE ON GE_KEYWORD.keyge_id = GE_KEYGE.id""", conn)
    DFWK.sort_values(by="word", inplace=True)
    DFWK.set_index("word")

    DFBL = pd.read_sql_query("""SELECT WORD FROM GE_BLACKLIST""", conn)
    DFBL.sort_values(by="word", inplace=True)
    DFBL.set_index("word")
    global v_blacklist
    v_blacklist = DFBL["word"].tolist()
    return DFWK, DFBL


if __name__ == "__main__":

    v_path = os.path.join(os.path.dirname(
        os.path.dirname(__file__)), "etl/config.txt")
    v_config = vr_config(v_path)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p",
        "--process",
        type=str,
        metavar="con",
        action="store",
        default=None,
        help="Run MapReducer from file to WORDMAP and KEYLINKS",
    )
    parser.add_argument(
        "-r",
        "--remap",
        type=str,
        metavar="ds",
        action="store",
        default=None,
        help="Run MapReducer from WORDMAP table",
    )
    parser.add_argument(
        "-k",
        "--file_keep",
        action="store_true",
        help="keep the source file in PSA folder",
    )
    parser.add_argument(
        "-w",
        "--file_wordmap",
        type=str,
        metavar="file",
        action="store",
        default=None,
        help="Output for WORDMAP will be a file and will not write to the database",
    )

    args = parser.parse_args()

    # debug variables
    args.process = "STRING_CLUSTER"
    args.connector = "STRING_CLUSTER"
    # ßargs.remap = "string_cluster"
    # args.file_keep = True
    # args.file_wordmap = "/Users/andrerico/hall/IGEM"

    if len(sys.argv) < 2:
        parser.print_usage()
        # sys.exit(2)

    # Remap or Process is allow
    if args.remap != None and args.process != None:
        print("Warning: Only remap or process is allow")

    # Check if path is valid
    if args.file_wordmap != None:
        if not os.path.exists(args.file_wordmap):
            print("ERROR: file is not available")
            sys.exit(1)

    if args.remap != None:

        conn = db_open(v_config["db_path"])

        DFWK, DFBL = db_masterdata(conn)

        if args.remap.upper() == "ALL":
            DFR = db_select_df("WORDMAP", args.remap.upper())
            if args.file_wordmap == None:
                db_delete("WORDMAP", args.remap.upper())
        else:
            DFR = db_select_df("WORDMAP", "DATASET='" +
                               args.remap.upper() + "'")
            if args.file_wordmap == None:
                db_delete("WORDMAP", "DATASET='" + args.remap.upper() + "'")

        if DFR.empty:
            print("WARNING: No records were found with the parameters informed")
            print("    Table: WORDMAP, DATASET:",
                  args.remap.upper(), "have 0 rows")
            print("    Process finished without remapping")
            conn.close()
            sys.exit(2)

        DFR.drop(["KEY1", "KEY2"], axis=1, inplace=True)

        # Apply Blacklist
        v_blacklist = DFBL["WORD"].tolist()
        DFR = DFR[~DFR["WORD1"].isin(v_blacklist)]
        DFR = DFR[~DFR["WORD2"].isin(v_blacklist)]

        DFR["KEY1"] = DFR.set_index("WORD1").index.map(
            DFWK.set_index("WORD")["KEY"])
        DFR["KEY2"] = DFR.set_index("WORD2").index.map(
            DFWK.set_index("WORD")["KEY"])

        print("STATUS: Remapping", len(DFR), "WORDMAP rows")

        if args.file_wordmap != None:
            DFR.to_csv(str(args.file_wordmap + "/wordmap.csv"))
            print("STATUS: file with WORDMAP created")
        else:
            try:
                DFR.to_sql("WORDMAP", conn, if_exists="append", index=False)
                conn.commit()
                print("STATUS: data writed in WORDMAP table")
            except:
                print("ERRO: It is not possible to write remap data in WORDMAP table")

        DFR.drop(["WORD1", "WORD2", "CONNECTOR"], axis=1, inplace=True)
        DFR.dropna(axis=0, inplace=True)

        if not DFR.empty:
            DFR = ingestor(DFR)

            print("STATUS: Reduced to", len(DFR.index), "KEYLINKS rows")

            if args.remap.upper() == "ALL":
                db_delete("KEYLINKS", args.remap.upper())
            else:
                db_delete("KEYLINKS", "DATASET='" + args.remap.upper() + "'")

            try:
                DFR.to_sql("KEYLINKS", conn, if_exists="append", index=False)
                conn.commit()
            except:
                print("ERRO: It is not possible to write remap data in KEYLINKS table")
        else:
            print("WARNING: No data to update KEYLINKS table")
        conn.close()

    if args.process != None:

        v_path_file = v_config["psa_path"]

        v_time = int(time.time())

        v_cores = os.cpu_count()
        print("STATUS: process MapReduce will run in", v_cores, "parallel cores")

        conn = db_open(v_config["db_path"])

        DFWK, DFBL = db_masterdata(conn)

        # v_blacklist = DFBL["word"].tolist()

        if args.process.upper() == "ALL":
            cursor = db_select_conn("GE_DATASET", "UPDATE_DS = TRUE")
        else:
            cursor = db_select_conn(
                "GE_DATASET",
                "UPDATE_DS = TRUE and DATASET='" + args.process.upper() + "'",
            )

        for row in cursor:
            print("STATUS: starting MapReducer on connector =", row[1])

            # config PSA folder (persistent staging area)
            v_dir = v_path_file + "/" + row[1]
            v_target_file = v_dir + "/" + row[8]
            if not os.path.exists(v_target_file):
                print("WARNING: file for mapping not available in " + v_target_file)
                continue

            df_reducer = pd.DataFrame(columns=["word1", "word2", "count"])

            with open(v_target_file) as fp:
                v_rows = math.ceil(len(fp.readlines()) / v_cores)
                print(
                    "STATUS: will process",
                    v_rows,
                    "rows in each of",
                    v_cores,
                    "parallel cores",
                )
                fp.close()

            # -------------------------------#
            # --- MAPPER ------------------- #
            # -------------------------------#
            with open(v_target_file) as fp:

                with concurrent.futures.ProcessPoolExecutor() as executor:
                    future = {
                        executor.submit(mapper, lines)
                        for lines in chunked_iterable(fp, v_rows)
                    }

                    for future_to in concurrent.futures.as_completed(future):
                        df_combiner = future_to.result()
                        df_reducer = pd.concat(
                            [df_reducer, df_combiner], axis=0)

            # -------------------------------#
            # --- REDUCER ------------------ #
            # -------------------------------#
            DFR = df_reducer.groupby(["word1", "word2"], as_index=False)[
                "count"].sum()

            # -------------------------------#
            # --- ADD WORDMAP TABLE  ------- #
            # -------------------------------#

            DFR["database"] = row[11]
            DFR["dataset"] = row[1]

            DFR["key1"] = DFR.set_index("word1").index.map(
                DFWK.set_index("word")["keyge"]
            )
            DFR["key2"] = DFR.set_index("word2").index.map(
                DFWK.set_index("word")["keyge"]
            )

            # DFR["keyge1_id"] = DFR.set_index("keyge1").index.map(
            #     DFWK.set_index("keyge")["id"]
            # )
            # DFR["keyge2_id"] = DFR.set_index("keyge2").index.map(
            #     DFWK.set_index("keyge")["id"]
            # )


            if args.file_wordmap != None:
                DFR.to_csv(str(args.file_wordmap +
                           "/wordmap-" + row[1] + ".csv"))
                print(
                    "STATUS: file with WORDMAP data created in",
                    args.file_wordmap + "/wordmap-" + row[1] + ".csv",
                )
            else:
                db_delete("X_WORDMAP", "DATASET='" + str(row[1]) + "'")

                conn_a = sqlite3.connect(
                    "/users/andrerico/dev/ge/ge-python/database/ge.db")

                # del DFR['keyge1']
                # del DFR['keyge2']
                DFR.to_sql("WORDMAP", conn_a, if_exists='append',
                           chunksize=1000, index=False)
                conn.commit()

                print("STATUS: data from",
                      row[1], "writed in WORDMAP table")
                try:
                    db_delete("GE_WORDMAP", "DATASET_ID='" + row[0] + "'")
                    DFR.to_sql("GE_WORDMAP", conn,
                               if_exists="append", index=False)
                    conn.commit()
                    print("STATUS: data from",
                          row[1], "writed in WORDMAP table")
                except:
                    print(
                        "ERRO: It is not possible to write",
                        row[1],
                        "data in WORDMAP table",
                    )

            DFR.drop(["word1", "word2"], axis=1, inplace=True)
            DFR.dropna(axis=0, inplace=True)

            if not DFR.empty:
                DFR = ingestor(DFR)

                print("STATUS: reduced to", len(DFR.index), "KEYLINKS rows")
                try:
                    db_delete("KEYLINKS", "DATASET='" + row[2] + "'")
                    DFR.to_sql("KEYLINKS", conn,
                               if_exists="append", index=False)
                    conn.commit()
                    print("STATUS: data from",
                          row[0], "writed in KEYLINKS table")
                except:
                    print(
                        "ERRO: It is not possible to write",
                        row[0],
                        "data in KEYLINKS table",
                    )
            else:
                print("WARNING: no data from",
                      row[0], "to update KEYLINKS table")

            if not args.file_keep:
                os.remove(v_target_file)
                print("STATUS: file", v_target_file, "was deleted")
            else:
                print("STATUS: keep source file in PSA folter")

            print("STATUS: finished", row[0], "process")

        conn.close()
        print("STATUS: all connectors was processed")
        print("STATUS: GE-db closed successfully")

print("END OF MAPREDUCER PROCESS")
