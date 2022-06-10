from pydoc import describe
from unicodedata import category
from django.core.management.base import BaseCommand
from ge.models import Blacklist, Category, Group, Keyge, Dataset, KeyLink, KeyWord, WordMap, Database
from django.conf import settings
import pandas as pd
import sys
import django
django.setup()


# future check: https://pypi.org/project/django-bulk-update-or-create/


class Command(BaseCommand):
    help = 'Loader is a interfa'

    def add_arguments(self, parser):
        # Positional arguments
        parser.add_argument('table', type=str)
    
        # Named (optional) arguments
        parser.add_argument(
            '--load',
            action='store_true',
            help='Load Master Data file to Database',
        )


    def handle(self, *args, **options):

        if options['load']:
            
            v_path_file = str(settings.BASE_DIR) + "/ge/psa/1_loader/" + options['table'] + ".csv"

            try:
                DFR = pd.read_csv(v_path_file)
            except IOError as e:
                print(e)
                sys.exit(2)

            print(DFR)

            if options['table'] == "blacklist":
                model_instances = [Blacklist(
                    word = record.word,
                    ) for record in DFR.itertuples()]
                Blacklist.objects.bulk_create(model_instances, ignore_conflicts=True)
            
            if options['table'] == "category":
                model_instances = [Category(
                    category = record.category,
                    description = record.description,
                    ) for record in DFR.itertuples()]
                Category.objects.bulk_create(model_instances, ignore_conflicts=True)

            if options['table'] == "group":
                model_instances = [Group(
                    group = record.group,
                    description = record.description,
                    ) for record in DFR.itertuples()]
                Group.objects.bulk_create(model_instances, ignore_conflicts=True) 

            if options['table'] == "database":
                model_instances = [Database(
                    database = record.database,
                    description = record.description,
                    category = record.category,
                    website = record.website,
                    ) for record in DFR.itertuples()]
                Database.objects.bulk_create(model_instances, ignore_conflicts=True)        

            if options['table'] == "keyge":         
                model_instances = [Keyge(
                    keyge = record.keyge,
                    category_id = record.category_id,
                    group_id = record.group_id,
                    description = record.description,
                    ) for record in DFR.itertuples()]
                Keyge.objects.bulk_create(model_instances, ignore_conflicts=True) 

            if options['table'] == "keyword":    
                DFK = pd.DataFrame(list(Keyge.objects.values()))
                print(DFK)

                DFR["keyge_id"] = DFR.set_index("keyge").index.map(DFK.set_index("keyge")["id"])

                print(DFR)

                model_instances = [KeyWord(
                    keyge_id = record.keyge_id,
                    word = record.word,
                    ) for record in DFR.itertuples()]
                KeyWord.objects.bulk_create(model_instances, ignore_conflicts=True) 


"""
Objetivo
- Carregar um arquivo CSV
- Adicionar os dados nas tabelas correspondentes
- Rotina para extrair os dados em CSV
- Rotina para exibir os dados??
"""