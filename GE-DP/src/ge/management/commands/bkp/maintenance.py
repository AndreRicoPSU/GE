from ge.models import Category, DSTColumn, Group, KeyHierarchy, KeyLink, Keyge, Dataset, KeyWord, Database, LogsCollector, PrefixOpc, WFControl, WordMap

from django.core.management.base import BaseCommand

""" 
Process to maintain the content of the Igem Database

Subprocess:
    1. Deletion of data from specific tables
    2. Deletion of all data from the IGEM database, except administration data such as users.
"""

class Command(BaseCommand):
    help = 'Process to maintain the content of the Igem Database'

    def add_arguments(self, parser):

        # Named (optional) arguments

        parser.add_argument(
            '--delete',
            type=str,
            metavar='table',
            action='store',
            default=None,
            help='Delete data on tables',
        )


    def handle(self, *args, **options):

        if options['delete']:
            #Only update registers will process = true
            if  options['delete'] == 'all':   
                KeyLink.truncate()
                WordMap.truncate()
                KeyWord.truncate()
                KeyHierarchy.truncate()
                Keyge.truncate()
                Category.truncate()
                Group.truncate()
                LogsCollector.truncate()
                WFControl.truncate()
                DSTColumn.truncate()
                PrefixOpc.truncate()
                Dataset.truncate()
                Database.truncate()

            if  options['delete'] == 'keylinks':  
                KeyLink.truncate()

            if  options['delete'] == 'wordmap':  
                WordMap.truncate()

            if  options['delete'] == 'keyword':                  
                KeyWord.truncate()
                
            if  options['delete'] == 'keyhierarchy':                  
                KeyHierarchy.truncate()

            if  options['delete'] == 'keyge':  
                Keyge.truncate()

            if  options['delete'] == 'category':  
                Category.truncate()

            if  options['delete'] == 'group':  
                Group.truncate()

            if  options['delete'] == 'logs':  
                LogsCollector.truncate()
                
            if  options['delete'] == 'workflow':  
                WFControl.truncate()

            if  options['delete'] == 'dst':  
                DSTColumn.truncate()

            if  options['delete'] == 'prefix':  
                PrefixOpc.truncate()

            if  options['delete'] == 'dataset':  
                Dataset.truncate()

            if  options['delete'] == 'database':  
                Database.truncate()
     

     