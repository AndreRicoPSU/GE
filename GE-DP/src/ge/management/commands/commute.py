import os
from django.conf import settings
from django.core.management.base import BaseCommand
from ge.models import Dataset, KeyWord, WFControl
from django.core.exceptions import ObjectDoesNotExist


""" 
The Data Commute process aims to search for words corresponding to the keys and eliminate words marked as blacklist

Subprocess:

Pendencies:
 - implement method with fragmented files to improve memory consumption
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

            #Only update registers will process = true
            if  options['run'] == 'all':           
                qs_queryset = Dataset.objects.filter(update_ds=True)
            else:
                try:
                    qs_queryset = Dataset.objects.filter(dataset = options['run'], update_ds=True)
                except ObjectDoesNotExist:
                    self.stdout.write(self.style.NOTICE('   Dataset not found'))
                if not qs_queryset:
                    self.stdout.write(self.style.NOTICE('   Dataset not found')) 

            ds_words = KeyWord.objects.filter(status=True, commute=True)

            for qs in qs_queryset:
  
                self.stdout.write(self.style.WARNING('Starting the dataset: %s' % qs.dataset))

                try:
                    qs_wfc = WFControl.objects.get(dataset_id = qs.id, chk_collect=True, chk_prepare=True, chk_commute=False)
                except ObjectDoesNotExist:
                    self.stdout.write(self.style.NOTICE('   Dataset without workflow to process'))
                    continue

                #variables                    
                v_dir = v_path_file + qs.dataset
                v_target = v_dir  + "/" + qs.dataset + ".csv"

                if not os.path.exists(v_target):
                    self.stdout.write(self.style.NOTICE('   File not available to:  "%s"' % qs.dataset))
                    self.stdout.write(self.style.NOTICE('   path:  "%s"' % v_target))
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

                # Update WorkFlow Control Process
                qs_wfc.chk_commute = True
                qs_wfc.save()

                self.stdout.write(self.style.SUCCESS('   Data Commute success to: %s' % qs.dataset))


        if options['reset']:
            if  options['reset'] == 'all':
                qs_wfc = WFControl.objects.all()
                qs_wfc.update(  chk_commute = False,
                                chk_mapreduce = False)                  
                self.stdout.write(self.style.SUCCESS('All datasets version control has been reset'))
            else:
                try:
                    qs_wfc = WFControl.objects.get(dataset_id__dataset = options['reset'])
                    qs_wfc.chk_commute = False
                    qs_wfc.chk_mapreduce = False
                    qs_wfc.save()                  
                    self.stdout.write(self.style.SUCCESS('dataset version control has been reset'))
                except ObjectDoesNotExist:
                    self.stdout.write(self.style.ERROR('Could not find dataset'))
          
               
