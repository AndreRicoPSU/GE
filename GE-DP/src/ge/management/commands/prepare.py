import os
import requests
import patoolib
import datetime
from django.conf import settings
from django.core.management.base import BaseCommand
from ge.models import Dataset, LogsCollector, DSTColumn
from django.utils import timezone
import pandas as pd
from django.core.exceptions import ObjectDoesNotExist
""" 
Second process in the data flow and aims to preparing the source data in an improved format before the MapReduce process

Subprocess:
    1. Elimination of header lines
    2. Deleting unnecessary columns
    3. Transforming ID columns with identifiers
    4. Replacement of terms

Pendencies:

Improves:
 - This first moment read all file, but improve to read in chunck to save memory 
 - Create a Workflow table to control each step of process (1. Collect; 2. Prepara; 3. MapReduce)
 - DSTColumn with better interface and control
 - Read n-files
 - Filter to run only one dataset
 - Argument to clean workflow per dataset

"""
class Command(BaseCommand):
    help = 'Preparation source data do MapReduce'

    def add_arguments(self, parser):
        # Positional arguments
        #parser.add_argument('ds_ids', nargs='+', type=str)
       
        # Named (optional) arguments
        parser.add_argument(
            '--process_all',
            action='store_true',
            help='Will process all active Datasets and with new version',
        )

    def handle(self, *args, **options):
        #config PSA folder (persistent staging area)
        v_path_file = str(settings.BASE_DIR) + "/psa/"


        if options['process_all']:

            #Only update registers will process = true
            ds_queryset = Dataset.objects.filter(update_ds=True)
  
            for ds in ds_queryset:
  
                self.stdout.write(self.style.SQL_FIELD('START:  "%s"' % ds.dataset))

                #variables                    
                v_dir = v_path_file + ds.dataset
                v_target_file = v_dir + "/" + ds.target_file_name
                v_target = v_dir  + "/" + ds.dataset + ".csv"
                v_skip = ds.source_file_skiprow
                v_tab = str(ds.source_file_sep)

                if not os.path.exists(v_target_file):
                    self.stdout.write(self.style.NOTICE('  File not available to:  "%s"' % ds.dataset))
                    self.stdout.write(self.style.ERROR('  path:  "%s"' % v_target_file))
                    continue
        
                             
                #Read file from PSA

                # 1. Elimination of header lines                
                df_source = pd.read_csv(v_target_file, sep=v_tab, skiprows=v_skip, engine='python')
                df_target = pd.DataFrame()
                print(df_source)

                v_col = len(df_source.columns)
       
                for n in range(v_col):

                    #Read transformations columns
                    try:
                        qs_col = DSTColumn.objects.get(dataset_id=ds.id, column_number=n)
                    except ObjectDoesNotExist:
                        qs_col = None

                    if not qs_col:
                        print('no rules defined')
                        print('check DSTColumn Table')
                        print('Column will consider on process')
                        df_target[n] = df_source.iloc[:,n]
                        # df_target[n].lower()
                    else:    
                        
                        if not qs_col.status:
                            print('Column is not active')
                        else:
                            # 3. Transforming ID columns with identifiers    
                            if qs_col.pre_choice:             
                                df_target[qs_col.column_name] = df_source.iloc[:,n].apply(lambda y: "{}{}".format(qs_col.pre_value,y))
                                # df_target[qs_col.column_name].lower()
                                continue
                            if qs_col.pos_choice:  
                                df_target[qs_col.column_name] = df_source.iloc[:,n].apply(lambda y: "{}{}".format(y,qs_col.pos_value))
                                # df_target[qs_col.column_name].lower()
                                continue      
                            if qs_col.replace_terms:
                                print("replacement terms is not implemeted yet")

                                continue 
                            df_target[qs_col.column_name] = df_source.iloc[:,n]
                            # df_target[qs_col.column_name].lower()
                            continue 
                    
                df_target = df_target.apply(lambda x: x.astype(str).str.lower()) # pode ser um ponto de lentidao no processo
                # pd.concat([df_target[col].astype(str).str.upper() for col in df_target.columns], axis=1)


                print(df_target)
                df_target.to_csv(v_target)
