import os
import pandas as pd
from django.conf import settings
from django.core.management.base import BaseCommand
from ge.models import Dataset, DSTColumn, WFControl
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

            for qs in qs_queryset:
                self.stdout.write(self.style.WARNING('Starting the dataset: %s' % qs.dataset))

                try:
                    qs_wfc = WFControl.objects.get(dataset_id = qs.id, chk_collect=True, chk_prepare=False)
                except ObjectDoesNotExist:
                    self.stdout.write(self.style.NOTICE('   Dataset without workflow to process'))
                    continue

                # Variables                    
                v_dir = v_path_file + qs.dataset
                v_target_file = v_dir + "/" + qs.target_file_name
                v_target = v_dir  + "/" + qs.dataset + ".csv"
                v_skip = qs.source_file_skiprow
                v_tab = str(qs.source_file_sep)

                if not os.path.exists(v_target_file):
                    self.stdout.write(self.style.NOTICE('   File not available to:  "%s"' % qs.dataset))
                    self.stdout.write(self.style.NOTICE('   path:  "%s"' % v_target_file))
                    continue
               
                # Read file from PSA

                # 1. Elimination of header lines                
                df_source = pd.read_csv(v_target_file, sep=v_tab, skiprows=v_skip, engine='python')
                df_target = pd.DataFrame()
               
                v_col = len(df_source.columns)
       
                for n in range(v_col):

                    # Read transformations columns
                    try:
                        qs_col = DSTColumn.objects.get(dataset_id=qs.id, column_number=n)
                    except ObjectDoesNotExist:
                        qs_col = None

                    if not qs_col:
                        self.stdout.write(self.style.WARNING('   No rules defines to column %s, check DSTColumn table. This column will consider on process' % n))
                        df_target[n] = df_source.iloc[:,n]
                    else:       
                        # 2. Deleting unnecessary columns 
                        if qs_col.status:  
                            # 3. Transforming ID columns with identifiers    
                            if qs_col.pre_value != 'none':             
                                df_target[qs_col.column_name] = df_source.iloc[:,n].apply(lambda y: "{}{}".format(qs_col.pre_value,y))
                                continue    
                               
                            df_target[qs_col.column_name] = df_source.iloc[:,n]
                            continue 

                # Keep all words lower case to match on db values on Commute and MapReduce process    
                df_target = df_target.apply(lambda x: x.astype(str).str.lower()) 

                # Update WorkFlow Control Process
                qs_wfc.chk_prepare = True
                qs_wfc.save()

                # Write the file
                df_target.to_csv(v_target)

                # Delete source file
                if not qs.target_file_keep: 
                    os.remove(v_target_file)

                self.stdout.write(self.style.SUCCESS('   Data preparation success to: %s' % qs.dataset))


        if options['reset']:
            if  options['reset'] == 'all':
                qs_wfc = WFControl.objects.all()
                qs_wfc.update(  chk_prepare = False,
                                chk_commute = False,
                                chk_mapreduce = False)                  
                self.stdout.write(self.style.SUCCESS('All datasets versio control has been reset'))
            else:
                try:
                    qs_wfc = WFControl.objects.get(dataset_id__dataset = options['reset'])
                    qs_wfc.chk_prepare = False
                    qs_wfc.chk_commute = False
                    qs_wfc.chk_mapreduce = False
                    qs_wfc.save()                  
                    self.stdout.write(self.style.SUCCESS('dataset version control has been reset'))
                except ObjectDoesNotExist:
                    self.stdout.write(self.style.ERROR('Could not find dataset'))