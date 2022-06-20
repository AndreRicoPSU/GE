import os
import requests
import patoolib
import datetime
from django.conf import settings
from django.core.management.base import BaseCommand
from ge.models import Dataset, LogsCollector, DSTColumn
from django.utils import timezone
import pandas as pd

"""
Pendencies: 
1. Add dataset controle to process only one
2. Another funcions reset / active / deactive / 
"""



class Command(BaseCommand):
    help = 'descrever sobre o modulo COLLECTOR'

    def add_arguments(self, parser):
        # Positional arguments
        #parser.add_argument('ds_ids', nargs='+', type=str)
       
        # Named (optional) arguments
        parser.add_argument(
            '--process',
            action='store_true',
            help='Will process routine to download db files from internet',
        )

        parser.add_argument(
            '--show',
            action='store_true',
            help='Will process routine to download db files from internet',
        )



    def handle(self, *args, **options):
        # config PSA folder (persistent staging area)
        v_path_file = str(settings.BASE_DIR) + "/psa/"

        # for ds in options['ds_ids']:
            # print(ds) 

        if options['process']:

            # Only update registers will process = true
            ds_queryset = Dataset.objects.filter(update_ds=True)
            #ds_queryset = ds_queryset.filter(dataset__contains='ds_ids')


            for ds in ds_queryset:
                self.stdout.write(self.style.SUCCESS('START:  "%s"' % ds.database))
                                    
                v_dir = v_path_file + ds.dataset
                v_file_url = ds.source_path
                v_source_file = v_dir + "/" + ds.source_file_name
                v_target_file = v_dir + "/" + ds.target_file_name

                # create folder to host file download
                if not os.path.isdir(v_dir):
                    os.makedirs(v_dir)
                    print("FOLDER  = ", v_dir)

                # Get file source version from ETAG
                try:
                    v_version = str(requests.get(v_file_url, stream=True).headers["etag"])
                except:
                    v_version = "0"
        
                # Check is new version before download
                if ds.source_file_version == v_version:
                    # Same vrsion, No will download
                    print("version already loaded")
                    log = LogsCollector(source_file_name = ds.source_file_name, 
                                        date = timezone.now(), # datetime.datetime.now(),
                                        dataset = ds.dataset,
                                        database = ds.database,
                                        version = v_version,
                                        status = False,
                                        size = 0) 
                    log.save()

                
                    print(str("'" + ds.source_file_sep + "'"))  


                else:
                    # New file version, start download
                    if os.path.exists(v_target_file):
                        os.remove(v_target_file)
                    if os.path.exists(v_source_file):
                        os.remove(v_source_file)
                    print("VERSION = ", "download in process ")
                    r = requests.get(v_file_url, stream=True)
                    with open(v_source_file, "wb") as download:
                        for chunk in r.iter_content(chunk_size=1000000):
                                if chunk:
                                    download.write(chunk)  # Improve Point
                    
                    # Update LOG table if new version
                    v_size = str(os.stat(v_source_file).st_size)
                    log = LogsCollector(source_file_name = ds.source_file_name, 
                                        date = timezone.now(), #datetime.datetime.now(),
                                        dataset = ds.dataset,
                                        database = ds.database,
                                        version = v_version,
                                        status = True,
                                        size = v_size) 
                    log.save()

                    # Unzip source file
                    if ds.source_compact:
                        patoolib.extract_archive(str(v_source_file), outdir=str(v_dir))
                        os.remove(v_source_file)

                    # release Dataset table:
                    ds.source_file_version = v_version
                    ds.source_file_size = v_size
                    ds.target_file_size = str(os.stat(v_target_file).st_size)
                    ds.last_update = timezone.now()#datetime.datetime.now()
                    ds.save()


                    # Transformation field to set with GE.db
                    # Omics fileds ID are compost only numbers and this process add a string to know this origem on MapRudecer process
                
                    v_skip = ds.source_file_skiprow
                    v_tab = str(ds.source_file_sep)
                    DFF = pd.read_csv(v_target_file, sep=v_tab, skiprows=v_skip, engine='python')

                    ds_queryset_columun = DSTColumn.objects.filter(dataset_id=ds.id, status=True) 

                    if ds_queryset_columun:
                        for ds_column in ds_queryset_columun:
                            x = int(ds_column.column_number)
                            if ds_column.pre_choice:             
                                DFF.iloc[:,x] = DFF.iloc[:,x].apply(lambda y: "{}{}".format(ds_column.pre_value,y))
                        
                            if ds_column.pos_choice:  
                                DFF.iloc[:,x] = DFF.iloc[:,x].apply(lambda y: "{}{}".format(y,ds_column.pos_value))

                        DFF.to_csv(v_target_file)   




        if options['show']:

            # Only update registers will process = true
            ds_queryset = Dataset.objects.all()
            #ds_queryset = ds_queryset.filter(dataset__contains='ds_ids')

            for ds in ds_queryset:
                self.stdout.write(self.style.SUCCESS(ds.database))
                print("  ID:",ds.id)
                print("  Database:",ds.database)
                print("  Last Update:",ds.last_update)
                print("  Uptade?:",ds.update_ds)                                    