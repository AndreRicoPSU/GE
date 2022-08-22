import os
import math
import pandas as pd
import re
import sys
from django.conf import settings
from ge.models import Dataset, KeyLink, Keyge, WordMap, WFControl
from django.core.exceptions import ObjectDoesNotExist
from concurrent.futures import as_completed
from django_thread import ThreadPoolExecutor
from itertools import combinations
from itertools import islice



""" 
MapReduce Process


SCHEMA
1. Will save all words on WORDMAP
2. Will save only Keyge on WORDMAP

Pendecies
- Better chunk control process

"""



def chunkify(df: pd.DataFrame, chunk_size: int):
    start = 0
    length = df.shape[0]

    # If DF is smaller than the chunk, return the DF
    if length <= chunk_size:
        yield list(df[:])
        return

    # Yield individual chunks
    while start + chunk_size <= length:
        yield (df[start:chunk_size + start])
        start = start + chunk_size

    # Yield the remainder chunk, if needed
    if start < length:
        yield (df[start:])



def mapper(lines):
        
        df_mapper = pd.DataFrame(columns=["word1", "word2", "count"])
        tmp = []
        
        for line in lines.itertuples(name=None, index=False):

            line = str(list(line)) # transf iterrows in string list     
            
            # Data Cleaning
            line = line.replace("'","") # delete ' between words inside string
            RE_DIGIT = re.compile(r"\b(?<![0-9-])(\d+)(?![0-9-])\b")      
            words = WORD_RE.findall(line)
            digits = RE_DIGIT.findall(str(words)) # Delete Numbers
            words.sort()
            words = list(set(words)) 
            words.sort()
            words = list(filter(lambda w: w not in digits, words)) #Delete words with only numbers

            # Mapping
            for (x, y) in combinations(words, 2):
                if x < y:
                    tmp.append([x, y, 1])
                else:
                    tmp.append([y, x, 1])
        df_mapper = pd.DataFrame(tmp, columns=["word1", "word2", "count"])
        
        return df_mapper



def MapRedProcess(v_ds, v_schema, v_chunk):

    if  v_ds == 'all':           
        qs_queryset = Dataset.objects.filter(update_ds=True)
    else:
        try:
            qs_queryset = Dataset.objects.filter(dataset = v_ds, update_ds=True)
        except ObjectDoesNotExist:
            print(' Dataset not found or inactivated')
        if not qs_queryset:
            print(' Dataset not found or inacticated')

    # config PSA folder (persistent staging area)
    v_path_file = str(settings.BASE_DIR) + "/psa/"

    global WORD_RE
    WORD_RE = re.compile(r"[\w'\:\#]+") # WORD_RE = re.compile(r"\b\d*[^\W\d_][^\W_]*\b")

    DFWK = pd.DataFrame(list(Keyge.objects.values('id','keyge')))
    if DFWK.empty:
        print(" The KEYGE table has no records.")
        if v_schema == 2:
            print(" It will not be possible to perform MapReduce without data in the KEYGE table with schema 2. \
                    Register new KEYGE or change to schema 1 in which all the words will save on WORDMAP")
            sys.exit(2)

    v_cores = os.cpu_count()
    print(" Process MapReduce will run in", v_cores, "parallel cores")

    for qs in qs_queryset:
        print('Starting the dataset: %s' % qs.dataset)

        # Check control proccess
        try:
            qs_wfc = WFControl.objects.get(dataset_id = qs.id, chk_collect=True, chk_prepare=True, chk_map=False)
        except ObjectDoesNotExist:
            print('  Dataset without workflow to process')
            continue

        v_dir = v_path_file + qs.dataset
        v_target = v_dir + "/" + qs.dataset + ".csv"
        if not os.path.exists(v_target):
            print("  File to process not available in " + v_target)
            continue
        
        

        idx = 0

        for fp in pd.read_csv(v_target, chunksize = v_chunk, low_memory=False, skipinitialspace=True):
         
            df_reducer = pd.DataFrame(columns=["word1", "word2", "count"])

            v_rows = math.ceil( len(fp.index) / v_cores )        
            print("  Chunking number: ", idx, ", rows per chunk: ", v_chunk, ", rows per core: ", v_rows)

            with ThreadPoolExecutor() as executor:
                future = {executor.submit(mapper, lines) for lines in chunkify(fp, v_rows)}

            for future_to in as_completed(future):
                df_combiner = future_to.result()
                df_reducer = pd.concat([df_reducer, df_combiner], axis=0)            
            print("   Finished Mapper")

            print("   Starting Reducer")
            DFR = df_reducer.groupby(["word1", "word2"], as_index=False)["count"].sum() 
            print("   Finished Reducer")
            
            print("   Starting link Words to Keyge")
            DFR["database_id"] = qs.database_id
            DFR["dataset_id"] = qs.id
            if DFWK.empty:
                print("    No data on Keyge table")
                DFR["keyge1_id"] = ""
                DFR["keyge2_id"] = ""
            else:
                DFR["keyge1_id"] = DFR.set_index("word1").index.map(DFWK.set_index("keyge")["id"])
                DFR["keyge2_id"] = DFR.set_index("word2").index.map(DFWK.set_index("keyge")["id"])
            print("   Finished link Words to Keyge")

            if v_schema == 2:
                print("   Cleaning records without Keygen")
                DFR.dropna(axis=0, inplace=True)
                
            DFR = DFR.where(pd.notnull(DFR), '')
            DFR.insert(loc=0, column="index", value=DFR.reset_index().index)
            
            print("   Writing on Database")
            if idx == 0: # first loop will delete all dataset registers on WORDMAP table
                WordMap.objects.filter(dataset_id = qs.id).delete() 
            
            # print(DFR)

            model_instances = [WordMap(
                cword = str(record.dataset_id) + '-' + str(idx) + '-' + str(record.index),
                word1 = record.word1,
                word2 = record.word2,   
                count = record.count,
                dataset_id = record.dataset_id,
                database_id = record.database_id,
                keyge1_id = record.keyge1_id,
                keyge2_id = record.keyge2_id,
                ) for record in DFR.itertuples()]

            WordMap.objects.bulk_create(model_instances)
            
            print("   Data from", qs.dataset, " and chunking number: ", idx, "saved to WORDMAP")
   
             # Loop control to debug            
            idx += 1
            #if idx > 1:
                # sys.exit(2)
                #break
        
        # Update WorkFlow Control Process
        qs_wfc.chk_map = True
        qs_wfc.save()

        print(" Finished", qs.dataset, "process")

    print("All dataset processed")
