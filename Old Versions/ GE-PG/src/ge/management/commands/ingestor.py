from django.core.management.base import BaseCommand
from ._process import process
import django
django.setup()

class Command(BaseCommand):
    help = 'descrever sobre o modulo INGESTOR'

    def add_arguments(self, parser):
        # Positional arguments
        #parser.add_argument('ds_ids', nargs='+', type=str)
    
        # Named (optional) arguments
        parser.add_argument(
            '--process',
            action='store_true',
            help='Will process routine to download db files from internet',
        )


    def handle(self, *args, **options):

        if options['process']:
            process()

        """
        Add another functions:
        -- Keep File
        -- Remap
        -- File WordMap
        """