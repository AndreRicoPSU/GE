import os
import math
import pandas as pd
import re
import sys
from django.conf import settings
from ge.models import Dataset, KeyLink, Keyge, WordMap
from django.core.exceptions import ObjectDoesNotExist
from concurrent.futures import as_completed
from django_thread import ThreadPoolExecutor
from itertools import combinations
from itertools import islice



""" 
MapReduce Process

Pendencies:

Improves:
 - WordMap will be opcional

"""

def chunked_iterable(iterable, size):
        while True:
            chunk = list(islice(iterable, size))
            if not chunk:
                break
            yield chunk


def mapper(lines):
        df_mapper = pd.DataFrame(columns=["word1", "word2", "count"])
        tmp = []
        
        for line in lines:

            RE_DIGIT = re.compile(r"\b(?<![0-9-])(\d+)(?![0-9-])\b")
            
            words = WORD_RE.findall(line)
  
            digits = RE_DIGIT.findall(str(words)) # Delete Numbers

            words.sort()
            words = list(set(words))  # Point for contestation
            words.sort()

            words = list(filter(lambda w: w not in digits, words)) #Delete words with only numbers

            # Mapping
            for (x, y) in combinations(words, 2):
                # if x != y: -->> works opposite with list(set(words))
                if x < y:
                    tmp.append([x, y, 1])
                else:
                    tmp.append([y, x, 1])
        df_mapper = pd.DataFrame(tmp, columns=["word1", "word2", "count"])

        return df_mapper


def MapRedProcess(v_ds):

    # CONFIGS
    # Create a file to handles this setup
    # # 0 = Read all words, delete word replace blacklist and create WordMap Output       
    # # 1 = Read only words with prefix and don`t create WordMap Output
    v_schema = 1        

    #Only update registers will process = true
    if  v_ds == 'all':           
        qs_queryset = Dataset.objects.filter(update_ds=True)
    else:
        try:
            qs_queryset = Dataset.objects.filter(dataset = v_ds, update_ds=True)
        except ObjectDoesNotExist:
            print('   Dataset not found')
        if not qs_queryset:
            print('   Dataset not found')


    # config PSA folder (persistent staging area)
    v_path_file = str(settings.BASE_DIR) + "/psa/"

    global WORD_RE
    WORD_RE = re.compile(r"[\w'\:\#]+") # WORD_RE = re.compile(r"\b\d*[^\W\d_][^\W_]*\b")

    v_cores = os.cpu_count()
    print("INFORM: process MapReduce will run in", v_cores, "parallel cores")

    DFWK = pd.DataFrame(list(Keyge.objects.values('id','keyge')))
 

 
    for qs in qs_queryset:
        print('Starting the dataset: %s' % qs.database)
                           
        v_dir = v_path_file + qs.dataset
        v_target = v_dir + "/" + qs.dataset + ".csv"

        if not os.path.exists(v_target):
            print("   file for MapReduce not available in " + v_target)
            continue
        
        df_reducer = pd.DataFrame(columns=["word1", "word2", "count"])

        with open(v_target) as fp:
            v_rows = math.ceil(len(fp.readlines()) / v_cores)
            print(
                "   Process",
                v_rows,
                "rows in each of",
                v_cores,
                "parallel cores",
            )
            fp.close()

        # -------------------------------#
        # --- MAPPER ------------------- #
        # -------------------------------#
        with open(v_target) as fp:

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
        DFR = df_reducer.groupby(["word1", "word2"], as_index=False)["count"].sum()

     
        DFR["database_id"] = qs.database_id
        DFR["dataset_id"] = qs.id


        if DFWK.empty:
            print("No data on Keyge")
            DFR["keyge1_id"] = ""
            DFR["keyge2_id"] = ""
        else:
            DFR["keyge1_id"] = DFR.set_index("word1").index.map(DFWK.set_index("keyge")["id"])
            DFR["keyge2_id"] = DFR.set_index("word2").index.map(DFWK.set_index("keyge")["id"])

        if v_schema == 0:
            WordMap.objects.filter(dataset_id = qs.id).delete()

        DFR = DFR.where(pd.notnull(DFR), '')
        
        DFR.insert(loc=0, column="index", value=DFR.reset_index().index)
           
        if v_schema == 0:   
            model_instances = [WordMap(
                cword = str(record.dataset_id) + '-' + str(record.index),
                word1 = record.word1,
                word2 = record.word2,   
                count = record.count,
                dataset_id = record.dataset_id,
                database_id = record.database_id,
                keyge1_id = record.keyge1_id,
                keyge2_id = record.keyge2_id,
                ) for record in DFR.itertuples()]

            WordMap.objects.bulk_create(model_instances)
        
            print("   Data from", qs.dataset, "writed in Wordmap table")
   

        # START KEYLINK
        if DFWK.empty:
            print("   No data on Keyge")
            sys.exit(2)
        
        DFR.drop(["word1", "word2", "index"], axis=1, inplace=True)
        DFR = DFR.replace('', pd.NaT)
        DFR.dropna(axis=0, inplace=True)

        DFR.keyge2_id = DFR.keyge2_id.astype(int)
        DFR.keyge1_id = DFR.keyge1_id.astype(int)

             
        if not DFR.empty:
        
            DFR = DFR.groupby(["dataset_id","keyge1_id","keyge2_id"], as_index=False)["count"].sum()

            DFR.insert(loc=0, column="index", value=DFR.reset_index().index)

            model_keylink = [KeyLink(
                ckey = str(str(record.dataset_id) + '-' + str(record.index)),
                dataset_id = record.dataset_id,
                keyge1_id =  record.keyge1_id,
                keyge2_id = record.keyge2_id,
                count = record.count,
            ) for record in DFR.itertuples()]

            KeyLink.objects.filter(dataset_id = qs.id).delete()
            KeyLink.objects.bulk_create(model_keylink)
           
        else:
            print("   No data from",
                    qs.dataset, "to update Keylink table")

        print("   Finished", qs.dataset, "process")

    print("All dataset was processed")

