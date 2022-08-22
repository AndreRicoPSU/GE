import os
import sys
import time
import pandas as pd
from django.conf import settings
from django.core.management.base import BaseCommand
from ge.models import Dataset, DSTColumn, WFControl, KeyWord
from django.core.exceptions import ObjectDoesNotExist


""" 
Second process in the data flow and aims to preparing the source data in an improved format before the MapReduce process

Subprocess:
    1. Elimination of header lines
    2. Deleting unnecessary columns
    3. Transforming ID columns with identifiers
    4. Replacement of terms
    5. Optional, delete source file

Pendencies:
 - This first moment read all file, but improve to read in chunck to save memory 
 - DSTColumn with better interface and control
 - Read n-files

"""



class Command(BaseCommand):
    help = 'Preparation source data do MapReduce'

    def add_arguments(self, parser):
       
        # Named (optional) arguments
        parser.add_argument(
            '--run',
            type=str,
            metavar='dataset',
            action='store',
            default=None,
            help='Will process active Datasets and with new version',
        )

        parser.add_argument(
            '--chunk',
            type=int,
            metavar='chunk size',
            action='store',
            default=1000000,
            help='Rows will be processed per cycle',
        ) 

        parser.add_argument(
            '--reset',
            type=str,
            metavar='dataset',
            action='store',
            default=None,
            help='Will reset dataset version control',
        )

    def handle(self, *args, **options):
        #config PSA folder (persistent staging area)
        v_path_file = str(settings.BASE_DIR) + "/psa/"

        if options['run']:
            v_time_process = time.time() 
            v_chunk = options['chunk']
            
            self.stdout.write(self.style.HTTP_NOT_MODIFIED('Start: Process to prepare and transformation external databases'))
            

            if  options['run'] == 'all': 
                v_where_cs = {'update_ds': True}
            else:
                v_where_cs = {'update_ds': True, 'dataset': options['run']}
            try:
                qs_queryset = Dataset.objects.filter(**v_where_cs)
            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Datasets not found or disabled'))
                sys.exit(2)
            if not qs_queryset:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Datasets not found or disabled'))
                sys.exit(2)


            # Only keywords with status and commute true
            # KeyWord table search the relationships between active words and key
            DF_KY_WD = pd.DataFrame(list(KeyWord.objects.values('word','keyge_id__keyge').filter(status=True, commute=True).order_by('word')))
            # adicionar um check se nao tem informacao nessa tabela
            if DF_KY_WD.empty:
                self.stdout.write(self.style.HTTP_NOT_FOUND('  No data on the relationship words and keyge'))
                # sys.exit(2)

            # Start process dataset 
            for qs in qs_queryset:
                self.stdout.write(self.style.HTTP_NOT_MODIFIED ('  Start: Run database {0} on dataset {1}'.format(qs.database, qs.dataset)))
                v_time_ds = time.time()
                

                # Check Workflow 
                try:
                    qs_wfc = WFControl.objects.get(dataset_id = qs.id, chk_collect=True, chk_prepare=False)
                except ObjectDoesNotExist:
                    self.stdout.write(self.style.HTTP_NOT_FOUND('    Dataset without workflow to process'))
                    continue

                # Variables                    
                v_dir = v_path_file + qs.dataset
                v_source = v_dir + "/" + qs.target_file_name
                v_target = v_dir  + "/" + qs.dataset + ".csv"
                v_skip = qs.source_file_skiprow
                v_tab = str(qs.source_file_sep)
                header = True

                # Check if file is available 
                if not os.path.exists(v_source):
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('    File not available to:  "%s"' % qs.dataset))
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('       path:  "%s"' % v_source))
                    continue
               
               # Delete exiting file
                if os.path.exists(v_target):
                    os.remove(v_target)

                if v_skip >= 1:  
                    v_read_file = {'sep': v_tab, 'skiprows': v_skip, 'engine': 'python', 'chunksize': v_chunk}
                else:
                    v_read_file = {'sep': v_tab, 'engine': 'python', 'chunksize': v_chunk}


               
                # 1. Elimination of header lines                
                try:
                    for df_source in pd.read_csv(v_source, **v_read_file):                                                                      
                            v_col = len(df_source.columns)
                            df_target = pd.DataFrame()
                            
                            for n in range(v_col):  # Read transformations columns
                                try:
                                    qs_col = DSTColumn.objects.get(dataset_id=qs.id, column_number=n)
                                    v_col_idx = str(qs_col.column_name)
                                except ObjectDoesNotExist:
                                    qs_col = None
    
                                if not qs_col:
                                    # Rule 1: Columns not configured on Dataset master data. The process will run and perform the Commute
                                    self.stdout.write(self.style.WARNING('   No rules defines to column %s, check DSTColumn table. This column will consider on process' % n))
                                    df_target[n] = df_source.iloc[:,n]
                                    df_target = df_target.apply(lambda x: x.astype(str).str.lower()) # Keep all words lower case to match 
                                    df_target[v_col_idx] = df_target.set_index(v_col_idx).index.map(DF_KY_WD.set_index("word")["keyge_id__keyge"]) # Commute Process
                                else:       
                                    if qs_col.status:  
                                        if str(qs_col.pre_value) != 'none':             
                                            # Rule 2: columns with defined prefixes / does not perform the commute process
                                            df_target[qs_col.column_name] = df_source.iloc[:,n].apply(lambda y: "{}{}".format(qs_col.pre_value,y))
                                            df_target = df_target.apply(lambda x: x.astype(str).str.lower()) # Keep all words lower case to match 
                                            continue        
                                        # Rule 3: Columns configured for the process with prefix None /Does not add prefix / Performs the Commute process
                                        df_target[qs_col.column_name] = df_source.iloc[:,n]
                                        df_target = df_target.apply(lambda x: x.astype(str).str.lower()) # Keep all words lower case to match 
                                        df_target[v_col_idx] = df_target.set_index(v_col_idx).index.map(DF_KY_WD.set_index("word")["keyge_id__keyge"]) # Commute Process
                                        continue    
                                # Rule 4: Columns configured and not activated will not be processed               
                            df_target.to_csv(v_target, header=header, mode='a') # Write the file
                            header = False # Prevent creating new header lines
                    
                    """
                    if v_skip >= 1:                                        
                        for df_source in pd.read_csv(v_source, sep=v_tab, skiprows=v_skip, engine='python', chunksize = v_chunk):                                                                      
                            v_col = len(df_source.columns)
                            df_target = pd.DataFrame()
                            
                            for n in range(v_col):  # Read transformations columns
                                try:
                                    qs_col = DSTColumn.objects.get(dataset_id=qs.id, column_number=n)
                                    v_col_idx = str(qs_col.column_name)
                                except ObjectDoesNotExist:
                                    qs_col = None
    
                                if not qs_col:
                                    # Rule 1: Columns not configured on Dataset master data. The process will run and perform the Commute
                                    self.stdout.write(self.style.WARNING('   No rules defines to column %s, check DSTColumn table. This column will consider on process' % n))
                                    df_target[n] = df_source.iloc[:,n]
                                    df_target = df_target.apply(lambda x: x.astype(str).str.lower()) # Keep all words lower case to match 
                                    df_target[v_col_idx] = df_target.set_index(v_col_idx).index.map(DF_KY_WD.set_index("word")["keyge_id__keyge"]) # Commute Process
                                else:       
                                    if qs_col.status:  
                                        if str(qs_col.pre_value) != 'none':             
                                            # Rule 2: columns with defined prefixes / does not perform the commute process
                                            df_target[qs_col.column_name] = df_source.iloc[:,n].apply(lambda y: "{}{}".format(qs_col.pre_value,y))
                                            df_target = df_target.apply(lambda x: x.astype(str).str.lower()) # Keep all words lower case to match 
                                            continue        
                                        # Rule 3: Columns configured for the process with prefix None /Does not add prefix / Performs the Commute process
                                        df_target[qs_col.column_name] = df_source.iloc[:,n]
                                        df_target = df_target.apply(lambda x: x.astype(str).str.lower()) # Keep all words lower case to match 
                                        df_target[v_col_idx] = df_target.set_index(v_col_idx).index.map(DF_KY_WD.set_index("word")["keyge_id__keyge"]) # Commute Process
                                        continue    
                                # Rule 4: Columns configured and not activated will not be processed               
                            df_target.to_csv(v_target, header=header, mode='a') # Write the file
                            header = False # Prevent creating new header lines

                    else:
                        # Improvement: I couldn't create a command to handle when we have skip 0 and greater than 0
                        for df_source in pd.read_csv(v_source, sep=v_tab, engine='python', chunksize = v_chunk):                                                                      
                            v_col = len(df_source.columns)
                            df_target = pd.DataFrame()
                            
                            for n in range(v_col):  # Read transformations columns
                                try:
                                    qs_col = DSTColumn.objects.get(dataset_id=qs.id, column_number=n)
                                    v_col_idx = str(qs_col.column_name)
                                except ObjectDoesNotExist:
                                    qs_col = None
    
                                if not qs_col:
                                    # Rule 1: Columns not configured on Dataset master data. The process will run and perform the Commute
                                    self.stdout.write(self.style.WARNING('   No rules defines to column %s, check DSTColumn table. This column will consider on process' % n))
                                    df_target[n] = df_source.iloc[:,n]
                                    df_target = df_target.apply(lambda x: x.astype(str).str.lower()) # Keep all words lower case to match 
                                    df_target[v_col_idx] = df_target.set_index(v_col_idx).index.map(DF_KY_WD.set_index("word")["keyge_id__keyge"]) # Commute Process
                                else:       
                                    if qs_col.status:  
                                        if str(qs_col.pre_value) != 'none':             
                                            # Rule 2: columns with defined prefixes / does not perform the commute process
                                            df_target[qs_col.column_name] = df_source.iloc[:,n].apply(lambda y: "{}{}".format(qs_col.pre_value,y))
                                            df_target = df_target.apply(lambda x: x.astype(str).str.lower()) # Keep all words lower case to match 
                                            continue        
                                        # Rule 3: Columns configured for the process with prefix None /Does not add prefix / Performs the Commute process
                                        df_target[qs_col.column_name] = df_source.iloc[:,n]
                                        df_target = df_target.apply(lambda x: x.astype(str).str.lower()) # Keep all words lower case to match 
                                        df_target[v_col_idx] = df_target.set_index(v_col_idx).index.map(DF_KY_WD.set_index("word")["keyge_id__keyge"]) # Commute Process
                                        continue    
                                # Rule 4: Columns configured and not activated will not be processed               
                            df_target.to_csv(v_target, header=header, mode='a') # Write the file
                            header = False # Prevent creating new header lines
                        """

                    # Update WorkFlow Control Process
                    qs_wfc.chk_prepare = True
                    qs_wfc.save()

                    # Delete source file
                    if not qs.target_file_keep: 
                        os.remove(v_source)

                    self.stdout.write(self.style.SUCCESS('   Data preparation success to: %s' % qs.dataset))
                                   
                except:
                    self.stdout.write(self.style.ERROR('   Error when process the : %s' % qs.dataset))
                    continue
               
             

        if options['reset']:
            if  options['reset'] == 'all':
                qs_wfc = WFControl.objects.all()
                qs_wfc.update(  chk_prepare = False,
                                chk_map = False,
                                chk_reducer = False)                  
                self.stdout.write(self.style.SUCCESS('All datasets versio control has been reset'))
            else:
                try:
                    qs_wfc = WFControl.objects.get(dataset_id__dataset = options['reset'])
                    qs_wfc.chk_prepare = False
                    qs_wfc.chk_map = False
                    qs_wfc.chk_reduce = False
                    qs_wfc.save()                  
                    self.stdout.write(self.style.SUCCESS('dataset version control has been reset'))
                except ObjectDoesNotExist:
                    self.stdout.write(self.style.ERROR('Could not find dataset'))