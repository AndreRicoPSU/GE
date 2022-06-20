import os
import requests
import patoolib
import datetime
from django.conf import settings
from django.core.management.base import BaseCommand
from ge.models import Dataset, LogsCollector, DSTColumn, KeyWord
from django.utils import timezone
import pandas as pd
from django.core.exceptions import ObjectDoesNotExist

""" 
Processo apos a preparacao dos dados e antes do mapreduce em que ira trocar os termos mapeados para o mesmo padrao

Subprocess:


Pendencies:
 - Melhorar o processo sem mais o wordmap
 - adicionar a blacklist

Improves:




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
  
            ds_words = KeyWord.objects.filter(status=True, commute=True)


            for ds in ds_queryset:
  
                self.stdout.write(self.style.SQL_FIELD('START:  "%s"' % ds.dataset))

                #variables                    
                v_dir = v_path_file + ds.dataset
                v_target = v_dir  + "/" + ds.dataset + ".csv"

                if not os.path.exists(v_target):
                    self.stdout.write(self.style.NOTICE('  File not available to:  "%s"' % ds.dataset))
                    self.stdout.write(self.style.ERROR('  path:  "%s"' % v_target))
                    continue
                        
                f = open(v_target,'r')
                filedata = f.read()
                f.close()
               
                for dsw in ds_words:
                    filedata = filedata.replace(str(dsw.word),str(dsw.keyge))

                #delete blacklist
                filedata = filedata.replace('blacklist','')            

                f = open(v_target,'w')
                f.write(filedata)
                f.close()

                             
               
