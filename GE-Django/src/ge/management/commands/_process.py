import os
import math
import pandas as pd
import re
from django.conf import settings
from ge.models import Blacklist, Dataset, KeyLink, KeyWord, WordMap
from concurrent.futures import as_completed
from django_thread import ThreadPoolExecutor
from itertools import combinations
from itertools import islice


def chunked_iterable(iterable, size):
        while True:
            chunk = list(islice(iterable, size))
            if not chunk:
                break
            yield chunk


def mapper(lines):
        df_mapper = pd.DataFrame(columns=["WORD1", "WORD2", "COUNT"])
        tmp = []
        for line in lines:
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
        df_mapper = pd.DataFrame(tmp, columns=["WORD1", "WORD2", "COUNT"])
        return df_mapper


def process():
    # config PSA folder (persistent staging area)
    v_path_file = str(settings.BASE_DIR) + "/ge/psa/"

    global WORD_RE, v_blacklist
    WORD_RE = re.compile(r"[\w']+") # WORD_RE = re.compile(r"\b\d*[^\W\d_][^\W_]*\b")

    v_cores = os.cpu_count()
    print("INFORM: process MapReduce will run in", v_cores, "parallel cores")

    DFWK = pd.DataFrame(list(KeyWord.objects.values()))
    DFBL = pd.DataFrame(list(Blacklist.objects.values()))
    v_blacklist = DFBL.word.tolist()  
    ds_queryset = Dataset.objects.filter(update_ds=True)
 
    for ds in ds_queryset:
        print('START:  "%s"' % ds.database)
                           
        v_dir = v_path_file + ds.dataset
        v_target_file = v_dir + "/" + ds.target_file_name

        if not os.path.exists(v_target_file):
            print("WARNING: file for mapping not available in " + v_target_file)
            continue
        
        df_reducer = pd.DataFrame(columns=["WORD1", "WORD2", "COUNT"])

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

            # with concurrent.futures.ProcessPoolExecutor() as executor:
            with ThreadPoolExecutor() as executor:
                future = {
                    executor.submit(mapper, lines)
                    for lines in chunked_iterable(fp, v_rows)
                }

                for future_to in as_completed(future):
                    df_combiner = future_to.result()
                    df_reducer = pd.concat([df_reducer, df_combiner], axis=0)

        # -------------------------------#
        # --- REDUCER ------------------ #
        # -------------------------------#
        DFR = df_reducer.groupby(["WORD1", "WORD2"], as_index=False)["COUNT"].sum()

     
        DFR["database_id"] = ds.database_id
        DFR["dataset_id"] = ds.id

        DFR["keyge1_id"] = DFR.set_index("WORD1").index.map(
            DFWK.set_index("word")["keyge_id"]
        )
        DFR["keyge2_id"] = DFR.set_index("WORD2").index.map(
            DFWK.set_index("word")["keyge_id"]
        )
        DFR['cword'] = str(ds.id) + str('-') + DFR['WORD1'] + str('-') + DFR['WORD2']
       
        WordMap.objects.filter(dataset_id = ds.id).delete()

        DFR = DFR.where(pd.notnull(DFR), '')
       
        model_instances = [WordMap(
            cword = record.cword,
            word1 = record.WORD1,
            word2 = record.WORD2,   
            count = record.COUNT,
            dataset_id = record.dataset_id,
            database_id = record.database_id,
            keyge1_id = record.keyge1_id,
            keyge2_id = record.keyge2_id,
            ) for record in DFR.itertuples()]

        WordMap.objects.bulk_create(model_instances)
    
        print("STATUS: data from", ds.dataset, "writed in WORDMAP table")
   
        DFR.drop(["WORD1", "WORD2"], axis=1, inplace=True)
        DFR = DFR.replace('', pd.NaT)
        DFR.dropna(axis=0, inplace=True)

        DFR.keyge2_id = DFR.keyge2_id.astype(int)
        DFR.keyge1_id = DFR.keyge1_id.astype(int)
      
        if not DFR.empty:
        
            DFR = DFR.groupby(["dataset_id","keyge1_id","keyge2_id"], as_index=False)["COUNT"].sum()
       
            model_keylink = [KeyLink(
                ckey = str(str(record.dataset_id) + '-' + str(record.keyge1_id) + '-' + str(record.keyge2_id)),
                dataset_id = record.dataset_id,
                keyge1_id =  record.keyge1_id,
                keyge2_id = record.keyge2_id,
                count = record.COUNT,
            ) for record in DFR.itertuples()]

            KeyLink.objects.filter(dataset_id = ds.id).delete()
            KeyLink.objects.bulk_create(model_keylink)
           
        else:
            print("WARNING: no data from",
                    ds.id, "to update KEYLINKS table")

        # if not args.file_keep:
        #     os.remove(v_target_file)
        #     print("STATUS: file", v_target_file, "was deleted")
        # else:
        #     print("STATUS: keep source file in PSA folter")

        print("STATUS: finished", ds.dataset, "process")

    print("STATUS: all dataset was processed")