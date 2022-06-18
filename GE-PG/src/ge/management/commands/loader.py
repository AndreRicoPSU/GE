from pydoc import describe
from tokenize import group
from unicodedata import category
from django.utils import timezone
from django.core.management.base import BaseCommand
from ge.models import Blacklist, Category, Group, Keyge, Dataset, KeyLink, KeyWord, WordMap, Database
from django.conf import settings
import pandas as pd
import sys
import django
django.setup()


# future check: https://pypi.org/project/django-bulk-update-or-create/

class Command(BaseCommand):
    help = 'Loader is a interface to write master data to GE Database'

    def add_arguments(self, parser):

        # Named (optional) arguments
        parser.add_argument(
            '--blacklist',
            action='store_true',
            help='Load Master Data file to Database',
        )
        parser.add_argument(
            '--category',
            action='store_true',
            help='Load Master Data file to Database',
        )
        parser.add_argument(
            '--group',
            action='store_true',
            help='Load Master Data file to Database',
        )
        parser.add_argument(
            '--database',
            action='store_true',
            help='Load Master Data file to Database',
        )
        parser.add_argument(
            '--keyge',
            action='store_true',
            help='Load Master Data file to Database',
        )
        parser.add_argument(
            '--keyword',
            action='store_true',
            help='Load Master Data file to Database',
        )
        parser.add_argument(
            '--dataset',
            action='store_true',
            help='Load Master Data file to Dataset',
        )

    def handle(self, *args, **options):

        if options['blacklist']:       
            v_path_file = str(settings.BASE_DIR) + "/loader/blacklist.csv"
            try:
                DFR = pd.read_csv(v_path_file)
            except IOError as e:
                self.stdout.write(self.style.ERROR('ERRO:')) 
                print(e)
                sys.exit(2)
            model_instances = [Blacklist(
                word = record.word,
                ) for record in DFR.itertuples()]
            Blacklist.objects.bulk_create(model_instances, ignore_conflicts=True)        
            self.stdout.write(self.style.SUCCESS('Load with success to Blacklist'))

        if options['category']:       
            v_path_file = str(settings.BASE_DIR) + "/loader/category.csv"
            try:
                DFR = pd.read_csv(v_path_file)
            except IOError as e:
                self.stdout.write(self.style.ERROR('ERRO:')) 
                print(e)
                sys.exit(2)
            model_instances = [Category(
                category = record.category,
                description = record.description,
                ) for record in DFR.itertuples()]
            Category.objects.bulk_create(model_instances, ignore_conflicts=True)
            self.stdout.write(self.style.SUCCESS('Load with success to Category'))

        if options['group']:       
            v_path_file = str(settings.BASE_DIR) + "/loader/group.csv"
            try:
                DFR = pd.read_csv(v_path_file)
            except IOError as e:
                self.stdout.write(self.style.ERROR('ERRO:')) 
                print(e)
                sys.exit(2)
            model_instances = [Group(
                group = record.group,
                description = record.description,
                ) for record in DFR.itertuples()]
            Group.objects.bulk_create(model_instances, ignore_conflicts=True) 
            self.stdout.write(self.style.SUCCESS('Load with success to Group'))

        if options['database']:       
            v_path_file = str(settings.BASE_DIR) + "/loader/database.csv"
            try:
                DFR = pd.read_csv(v_path_file)
            except IOError as e:
                self.stdout.write(self.style.ERROR('ERRO:')) 
                print(e)
                sys.exit(2)
            model_instances = [Database(
                database = record.database,
                description = record.description,
                category = record.category,
                website = record.website,
                ) for record in DFR.itertuples()]
            Database.objects.bulk_create(model_instances, ignore_conflicts=True)        
            self.stdout.write(self.style.SUCCESS('Load with success to Database'))




        if options['dataset']:
            v_path_file = str(settings.BASE_DIR) + "/loader/dataset.csv"
            try:
                DFR = pd.read_csv(v_path_file)
            except IOError as e:
                self.stdout.write(self.style.ERROR('ERRO:')) 
                print(e)
                sys.exit(2)
            DFDB = pd.DataFrame(list(Database.objects.values()))
            DFR["db_id"] = DFR.set_index("database").index.map(DFDB.set_index("database")["id"])
            model_instances = [Dataset(
                dataset = record.dataset,
                database_id = record.db_id,
                description = record.description,
                update_ds = record.update_ds,
                last_update = timezone.now(),
                source_path = record.source_path,
                source_web = record.source_web,
                source_compact = record.source_compact,
                source_file_name = record.source_file_name,
                source_file_format = record.source_file_format,
                source_file_sep = record.source_file_sep,
                source_file_skiprow = record.source_file_skiprow,
                source_file_version = 0,
                source_file_size = 0,
                target_file_name = record.target_file_name,
                target_file_format = record.target_file_format,
                target_file_size = 0,
            ) for record in DFR.itertuples()]
            Dataset.objects.bulk_create(model_instances, ignore_conflicts=True)        
            self.stdout.write(self.style.SUCCESS('Load with success to Dataset'))


        if options['keyge']:       
            v_path_file = str(settings.BASE_DIR) + "/loader/keyge.csv"
            try:
                DFR = pd.read_csv(v_path_file)
            except IOError as e:
                self.stdout.write(self.style.ERROR('ERRO:')) 
                print(e)
                sys.exit(2)

            DFG = pd.DataFrame(list(Group.objects.values()))
            DFC = pd.DataFrame(list(Category.objects.values()))
            DFR["group_id"] = DFR.set_index("group").index.map(DFG.set_index("group")["id"])
            DFR["category_id"] = DFR.set_index("category").index.map(DFC.set_index("category")["id"])
            if DFR.isnull().values.any():
                self.stdout.write(self.style.ERROR('Group and/or Category was not match on Database')) 
                sys.exit(2)

            model_instances = [Keyge(
                keyge = record.keyge,
                category_id = record.category_id,
                group_id = record.group_id,
                description = record.description,
                ) for record in DFR.itertuples()]
            Keyge.objects.bulk_create(model_instances, ignore_conflicts=True) 
            self.stdout.write(self.style.SUCCESS('Load with success to Keyge'))
            

        "add check integriteS"
        if options['keyword']:       
            v_path_file = str(settings.BASE_DIR) + "/loader/keyword.csv"
            try:
                DFR = pd.read_csv(v_path_file)
            except IOError as e:
                self.stdout.write(self.style.ERROR('ERRO:')) 
                print(e)
                sys.exit(2)
            DFK = pd.DataFrame(list(Keyge.objects.values()))
            DFR["keyge_id"] = DFR.set_index("keyge").index.map(DFK.set_index("keyge")["id"])
            if DFR.isnull().values.any():
                self.stdout.write(self.style.ERROR('Keyge was not match on Database')) 
                sys.exit(2)
            
            model_instances = [KeyWord(
                keyge_id = record.keyge_id,
                word = record.word,
                ) for record in DFR.itertuples()]
            KeyWord.objects.bulk_create(model_instances, ignore_conflicts=True) 
            self.stdout.write(self.style.SUCCESS('Load with success to Key-Words'))