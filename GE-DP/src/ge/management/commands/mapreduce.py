from django.core.management.base import BaseCommand
from ._process import process
from ._mapred import MapRedProcess
from ge.models import WFControl
from django.core.exceptions import ObjectDoesNotExist
import django
django.setup()

""" 
Process in the data flow and aims to run MapReduce process to link to words in same column

Subprocess:

Pendencies:
Improves:

"""

class Command(BaseCommand):
    help = 'Run MapReducer from files after prepare to integrate on WordMap and Keylink'

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

    def handle(self, *args, **options):

        # #old process
        # if options['process']:
        #     process()

        # new process
        if options['run']:
            MapRedProcess(options['run'])


        if options['reset']:
            if  options['reset'] == 'all':
                qs_wfc = WFControl.objects.all()
                qs_wfc.update(chk_mapreduce = False)                  
                self.stdout.write(self.style.SUCCESS('All datasets version control has been reset'))
            else:
                try:
                    qs_wfc = WFControl.objects.get(dataset_id__dataset = options['reset'])
                    qs_wfc.chk_mapreduce = False
                    qs_wfc.save()                  
                    self.stdout.write(self.style.SUCCESS('dataset version control has been reset'))
                except ObjectDoesNotExist:
                    self.stdout.write(self.style.ERROR('Could not find dataset'))