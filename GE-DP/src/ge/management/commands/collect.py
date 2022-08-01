from curses import update_lines_cols
import os
from turtle import update
import requests
import patoolib
from django.conf import settings
from django.core.management.base import BaseCommand
from ge.models import Dataset, LogsCollector, WFControl
from django.utils import timezone
from django.core.exceptions import ObjectDoesNotExist


""" 
First process in the data flow and aims to extract new versions of external databases for the PSA area

Version Control Rule:   Collect only considers datasets marked as active and different version control
                        To reprocess a database with same version, use reset and run the dataset

Options:
--run_all       ==> Consider all active datasets to collect the files. 
--run "ds"      ==> Consider just one dataset to collect the file.
--reset_all     ==> Reset version control to all datasets.
--reset "ds"    ==> Reset version control just one dataset.
--show          ==> Print all datasets
--active "ds"   ==> Active a dataset to collect the files
--deactive "ds" ==> Deactive a dataset to collect the files


Pendencies
 - Create setting to active logs
 - Subprocess to run one dataset
"""

class Command(BaseCommand):
    help = 'Collect external databases to PSA'

    def add_arguments(self, parser):
       
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

        parser.add_argument(
            '--show',
            action='store_true',
            help='Will show the Master Data Datasets',
        )

        parser.add_argument(
            '--activate',
            type=str,
            metavar='dataset',
            action='store',
            default=None,
            help='',
        )

        parser.add_argument(
            '--deactivate',
            type=str,
            metavar='dataset',
            action='store',
            default=None,
            help='',
        )

    def handle(self, *args, **options):
        # Config PSA folder (persistent staging area)
        v_path_file = str(settings.BASE_DIR) + "/psa/"

        if options['run']:
            # Only update registers will process = true
            if  options['run'] == 'all':           
                qs_queryset = Dataset.objects.filter(update_ds=True)
            else:
                try:
                    qs_queryset = Dataset.objects.filter(dataset = options['run'], update_ds=True)
                except ObjectDoesNotExist:
                    self.stdout.write(self.style.NOTICE('   Dataset not found'))
                if not qs_queryset:
                    self.stdout.write(self.style.NOTICE('   Dataset not found')) 

            for qs in qs_queryset:
                self.stdout.write(self.style.WARNING('Starting the dataset: %s' % qs.dataset))

                # Variables                    
                v_dir = v_path_file + qs.dataset
                v_file_url = qs.source_path
                v_source_file = v_dir + "/" + qs.source_file_name
                v_target_file = v_dir + "/" + qs.target_file_name

                # Create folder to host file download
                if not os.path.isdir(v_dir):
                    os.makedirs(v_dir)
                    print("   Folder created to host the files in ", v_dir)

                # Get file source version from ETAG
                try:
                    v_version = str(requests.get(v_file_url, stream=True).headers["etag"])
                except:
                    v_version = "0"
                    self.stdout.write(self.style.WARNING("   Could not find the version of the file. Assigned a random version"))
                
                # Get WorkFlow Control
                try:
                    qs_wfc = WFControl.objects.get(dataset_id = qs.id)
                except ObjectDoesNotExist:
                    qs_control = WFControl(
                        dataset_id = qs.id,
                        last_update = timezone.now(),
                        source_file_version = 0,
                        source_file_size = 0,
                        target_file_size = 0,
                        chk_collect = False,
                        chk_prepare = False,
                        chk_commute = False,
                        chk_mapreduce = False
                    )
                    qs_control.save()
                    qs_wfc = WFControl.objects.get(dataset_id = qs.id)

                # Check is new version before download
                if qs_wfc.source_file_version == v_version:
                    # Same vrsion, only write the log table
                    # Create a LOG setting control (optional to log control)
                    # log = LogsCollector(source_file_name = qs.source_file_name, 
                    #                     date = timezone.now(),
                    #                     dataset = qs.dataset,
                    #                     database = qs.database,
                    #                     version = v_version,
                    #                     status = False,
                    #                     size = 0) 
                    # log.save() 
                    self.stdout.write(self.style.MIGRATE_LABEL('   Version already loaded in: %s' % qs_wfc.last_update))          
                
                # New file version, start download
                else:   
                    if os.path.exists(v_target_file):
                        os.remove(v_target_file)
                    if os.path.exists(v_source_file):
                        os.remove(v_source_file)

                    self.stdout.write(self.style.MIGRATE_LABEL('   Starting download'))           
                    r = requests.get(v_file_url, stream=True)
                    with open(v_source_file, "wb") as download:
                        for chunk in r.iter_content(chunk_size=1000000):
                                if chunk:
                                    download.write(chunk)  # Improve Point
                    
                    # Update LOG table if new version
                    v_size = str(os.stat(v_source_file).st_size)
                    # Create a LOG setting control (optional to log control)
                    # log = LogsCollector(source_file_name = qs.source_file_name, 
                    #                     date = timezone.now(), #datetime.datetime.now(),
                    #                     dataset = qs.dataset,
                    #                     database = qs.database,
                    #                     version = v_version,
                    #                     status = True,
                    #                     size = v_size) 
                    # log.save()
                    self.stdout.write(self.style.SUCCESS('   Download successful'))

                    # Unzip source file
                    if qs.source_compact:
                        try:
                            self.stdout.write(self.style.MIGRATE_LABEL('   Starting the file unzipping process'))
                            patoolib.extract_archive(str(v_source_file), outdir=str(v_dir))
                            os.remove(v_source_file)
                        except:
                            self.stdout.write(self.style.MIGRATE_LABEL('   There was a failure to unzip the file'))

                    # Check if target file is ok
                    if not os.path.exists(v_target_file):
                        self.stdout.write(self.style.ERROR('   error reading the file after unzipping. Possible cause: check if the names of the source and destination files are correct in the dataset table.'))
                        qs_wfc.source_file_version = "ERROR"
                        qs_wfc.last_update = timezone.now()
                        qs_wfc.save()
                        for i in os.listdir(v_dir):
                            os.remove(v_dir + "/" + i)
                        continue

                    # Update WorkFlow Control table:
                    self.stdout.write(self.style.MIGRATE_LABEL('   Updating Dataset Control Data'))
                    qs_wfc.source_file_version = v_version
                    qs_wfc.source_file_size = v_size
                    qs_wfc.target_file_size = str(os.stat(v_target_file).st_size)
                    qs_wfc.last_update = timezone.now()
                    qs_wfc.chk_collect = True
                    qs_wfc.chk_prepare = False
                    qs_wfc.chk_commute = False
                    qs_wfc.mapreduce = False
                    qs_wfc.save()


        if options['reset']:
            if  options['reset'] == 'all':
                qs_wfc = WFControl.objects.all()
                qs_wfc.update(last_update = timezone.now(),
                                source_file_version = 0,
                                source_file_size = 0,
                                target_file_size = 0,
                                chk_collect = False,
                                chk_prepare = False,
                                chk_commute = False,
                                chk_mapreduce = False)                  
                self.stdout.write(self.style.SUCCESS('All dataset versio control has been reset'))
            else:
                try:
                    qs_wfc = WFControl.objects.get(dataset_id__dataset = options['reset'])
                    qs_wfc.last_update = timezone.now()
                    qs_wfc.source_file_version = 0
                    qs_wfc.source_file_size = 0
                    qs_wfc.target_file_size = 0
                    qs_wfc.chk_collect = False
                    qs_wfc.chk_prepare = False
                    qs_wfc.chk_commute = False
                    qs_wfc.chk_mapreduce = False
                    qs_wfc.save()                  
                    self.stdout.write(self.style.SUCCESS('dataset versio control has been reset'))
                except ObjectDoesNotExist:
                    self.stdout.write(self.style.ERROR('Could not find dataset'))





        if options['show']:
            qs_queryset = Dataset.objects.all()
            for qs in qs_queryset:
                self.stdout.write(self.style.HTTP_INFO(qs.database))
                print("  ID:",qs.id)
                print("  Database:",qs.database)
                print("  Status:",qs.update_ds)     

        if options['activate']:
            try:
                qs_wfc = Dataset.objects.get(dataset = options['activate'])
                qs_wfc.update_ds=True
                qs_wfc.save() 
                self.stdout.write(self.style.SUCCESS('dataset activated'))
            except ObjectDoesNotExist:
                self.stdout.write(self.style.ERROR('Could not find dataset'))

        if options['deactivate']:
            try:
                qs_wfc = Dataset.objects.get(dataset = options['deactivate'])
                qs_wfc.update_ds=False
                qs_wfc.save() 
                self.stdout.write(self.style.SUCCESS('dataset dactivated'))
            except ObjectDoesNotExist:
                self.stdout.write(self.style.ERROR('Could not find dataset'))
                          