from django.core.management.base import BaseCommand
from ._process import process
from ._mapred import MapRedProcess
import django
django.setup()

""" 
third process in the data flow and aims to run MapReduce process to link to words in same column

Subprocess:
    1. process

Pendencies:

Improves:
        -- Keep File
        -- Remap
        -- File WordMap

"""

class Command(BaseCommand):
    help = 'Run MapReducer from files after prepare to integrate on WordMap and Keylink'

    def add_arguments(self, parser):
        # Positional arguments
        #parser.add_argument('ds_ids', nargs='+', type=str)
    
        # Named (optional) arguments
        parser.add_argument(
            '--process',
            action='store_true',
            help='Will process routine to download db files from internet',
        )

                # Named (optional) arguments
        parser.add_argument(
            '--run',
            action='store_true',
            help='Will process routine to download db files from internet',
        )


    def handle(self, *args, **options):

        #old process
        if options['process']:
            process()

        # new process
        if options['run']:
            MapRedProcess()

