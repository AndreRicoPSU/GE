
import time
from tokenize import group
from ge.models import Category, DSTColumn, Group, KeyHierarchy, KeyLink, Keyge, Dataset, KeyWord, Database, LogsCollector, PrefixOpc, WFControl, WordMap

from django.core.management.base import BaseCommand

import os
import sys
import time
import pandas as pd
from django.conf import settings
from django.core.management.base import BaseCommand
from ge.models import Dataset, DSTColumn, WFControl, KeyWord
from django.core.exceptions import ObjectDoesNotExist



""" 
Process to maintain the content of the Igem Database

--subprocess:
    1. show
    2. delete
    3. download
    4. load

{tables}
    1. database
    2. dataset
    3. ds_columns
    4. workflow
    5. keyge
    6. key_category
    7. key_group
    8. key_prefix
    9. key_word
    10. link_key
    11. link_word

--field
    1. database
    2. dataset
    3. keyge
    4. category
    5. group
    6. word

{field_value}
    Open values

--path
    file path and name to read and write from ge.db

Sintaxes:
python manage.py db {--subprocess} {table} {--field} {field_value} {--path} {file_path}

    python manage.py db --show workflow --dataset all
    python manage.py db --delete dataset --dataset all
    python manage.py db --download keyge --keyge all --path xxxx
    python manage.py db --load dataset --path xxxxx


Premissas
    Mesmo layout entre o download
    Validar os campos
"""




class Command(BaseCommand):
    help = 'Process to maintain the content of the Igem Database'

    def add_arguments(self, parser):


        # subpreocess
        parser.add_argument(
            '--show',
            type=str,
            metavar='table',
            action='store',
            default=None,
            help='show data on tables',
        )
        parser.add_argument(
            '--delete',
            type=str,
            metavar='table',
            action='store',
            default=None,
            help='Delete data on tables',
        )
        parser.add_argument(
            '--download',
            type=str,
            metavar='table',
            action='store',
            default=None,
            help='read data on tables and create a file output',
        )
        parser.add_argument(
            '--load',
            type=str,
            metavar='table',
            action='store',
            default=None,
            help='write data on tables from a file',
        )

        # Fields
        parser.add_argument(
            '--database',
            type=str,
            metavar='database',
            action='store',
            default='all',
            help='database value',
        )
        parser.add_argument(
            '--dataset',
            type=str,
            metavar='dataset',
            action='store',
            default='all',
            help='dataset value',
        )
        parser.add_argument(
            '--keyge',
            type=str,
            metavar='keyge',
            action='store',
            default='all',
            help='keyge value',
        )
        parser.add_argument(
            '--category',
            type=str,
            metavar='category',
            action='store',
            default='all',
            help='category value',
        )
        parser.add_argument(
            '--group',
            type=str,
            metavar='group',
            action='store',
            default='all',
            help='group value',
        )
        parser.add_argument(
            '--word',
            type=str,
            metavar='word',
            action='store',
            default='all',
            help='group value',
        )

        # File Path
        parser.add_argument(
            '--path',
            type=str,
            metavar='file path',
            action='store',
            default=None,
            help='group value',
        )


    def handle(self, *args, **options):


        def get_model_field_names(model, ignore_fields=['content_object']):
                    model_fields = model._meta.get_fields()
                    model_field_names = list(set([f.name for f in model_fields if f.name not in ignore_fields]))
                    return model_field_names

        def get_lookup_fields(model, fields=None):
            model_field_names = get_model_field_names(model)
            if fields is not None:
                lookup_fields = []
                for x in fields:
                    if "__" in x:
                        # the __ is for ForeignKey lookups
                        lookup_fields.append(x)
                    elif x in model_field_names:
                        lookup_fields.append(x)
            else:
                lookup_fields = model_field_names
            return lookup_fields

        def qs_to_dataset(qs, fields=None):       
            lookup_fields = get_lookup_fields(qs.model, fields=fields)
            return list(qs.values(*lookup_fields))

        def convert_to_dataframe(qs, fields=None, index=None):
            lookup_fields = get_lookup_fields(qs.model, fields=fields)
            index_col = None
            if index in lookup_fields:
                index_col = index
            elif "id" in lookup_fields:
                index_col = 'id'
            values = qs_to_dataset(qs, fields=fields)
            df = pd.DataFrame.from_records(values, columns=lookup_fields, index=index_col)
            return df

        def get_database(*args):
            if v_database != 'all': 
                v_where_cs = {'database': v_database}
            else:
                v_where_cs = {}
            try:
                qs_database = Database.objects.filter(**v_where_cs).order_by('database')
            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Database not found'))
                sys.exit(2)
            if not qs_database:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Database not found'))
                sys.exit(2)
            return qs_database

        def get_dataset(*args):
            if v_database != 'all':
                try:
                    QS_DB = Database.objects.filter(database = v_database)
                    for qs in QS_DB:
                        v_db_id = qs.id
                except:
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Database not found'))
                    sys.exit(2)
                if not QS_DB:                     
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Database not found'))
                    sys.exit(2)
            if v_database != 'all' and v_dataset != 'all': 
                v_where_cs = {'database': v_db_id, 'dataset': v_dataset}
            elif v_database == 'all' and v_dataset != 'all': 
                v_where_cs = {'dataset': v_dataset}
            elif v_database != 'all' and v_dataset == 'all': 
                v_where_cs = {'database': v_db_id}                
            else:
                v_where_cs = {}
            try:
                QS = Dataset.objects.filter(**v_where_cs).order_by('database','dataset')
            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Dataset not found'))
                sys.exit(2)
            if not QS:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Dataset not found'))
                sys.exit(2)
            return QS

        def get_ds_column(*args):
            if v_dataset != 'all':
                try:
                    QS_DB = Dataset.objects.filter(dataset = v_dataset)
                    for qs in QS_DB:
                        v_db_id = qs.id
                except:
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Dataset not found'))
                    sys.exit(2)
                if not QS_DB:                     
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Dataset not found'))
                    sys.exit(2)
            if v_dataset != 'all': 
                v_where_cs = {'dataset': v_db_id}
            else:
                v_where_cs = {}
            try:
                QS = DSTColumn.objects.filter(**v_where_cs).order_by('dataset')
            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Dataset not found'))
                sys.exit(2)
            if not QS:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Dataset not found'))
                sys.exit(2)
            return QS

        def get_workflow(*args):
            if v_dataset != 'all':
                try:
                    QS_DB = Dataset.objects.filter(dataset = v_dataset)
                    for qs in QS_DB:
                        v_db_id = qs.id
                except:
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Dataset not found'))
                    sys.exit(2)
                if not QS_DB:                     
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Dataset not found'))
                    sys.exit(2)
            if v_dataset != 'all': 
                v_where_cs = {'dataset': v_db_id}
            else:
                v_where_cs = {}
            try:
                QS = WFControl.objects.filter(**v_where_cs).order_by('dataset')
            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Dataset not found'))
                sys.exit(2)
            if not QS:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Dataset not found'))
                sys.exit(2)
            return QS            

        def get_keyge(*args):
            if v_group != 'all':
                try:
                    QS_DB = Group.objects.filter(group = v_group)
                    for qs in QS_DB:
                        v_id_group = qs.id     
                except:
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Group not found'))
                    sys.exit(2)
                if not QS_DB:                     
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Group not found'))
                    sys.exit(2)
            if v_category != 'all':
                try:
                    QS_DB = Category.objects.filter(category = v_category)
                    for qs in QS_DB:
                        v_id_cat = qs.id     
                except:
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Category not found'))
                    sys.exit(2)
                if not QS_DB:                     
                    self.stdout.write(self.style.HTTP_BAD_REQUEST('  Category not found'))
                    sys.exit(2)
            if v_group != 'all' and v_category != 'all': 
                v_where_cs = {'group': v_id_group, 'category': v_id_cat}
            elif v_group == 'all' and v_category != 'all': 
                v_where_cs = {'category': v_id_cat}
            elif v_group != 'all' and v_category == 'all': 
                v_where_cs = {'group': v_id_group}                
            else:
                v_where_cs = {}
            try:
                QS = Keyge.objects.filter(**v_where_cs).order_by('group','category','keyge')
            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Keyge not found'))
                sys.exit(2)
            if not QS:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Keyge not found'))
                sys.exit(2)
            return QS

        def get_category(*args):
            try:
                QS = Category.objects.all().order_by('category')
            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Category not found'))
                sys.exit(2)
            if not QS:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Category not found'))
                sys.exit(2)
            return QS

        def get_group(*args):
            try:
                QS = Group.objects.all().order_by('group')
            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Group not found'))
                sys.exit(2)
            if not QS:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Group not found'))
                sys.exit(2)
            return QS

        def get_prefix(*args):
            try:
                QS = PrefixOpc.objects.all().order_by('pre_value')
            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Prefix not found'))
                sys.exit(2)
            if not QS:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Prefix not found'))
                sys.exit(2)
            return QS

        def get_key_word(*args):
            if v_word != 'all':
                v_where_cs = {'word__contains': v_word } # %like%
            else:
                v_where_cs = {}
            try:
                QS = KeyWord.objects.filter(**v_where_cs).order_by('keyge','word')
            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Word not found'))
                sys.exit(2)
            if not QS:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Word not found'))
                sys.exit(2)
            return QS

        def get_link_key(*args):
            if v_word != 'all':
                v_where_cs = {'word__contains': v_word } # %like%
            else:
                v_where_cs = {}
            try:
                QS = KeyLink.objects.values('ckey', 'dataset', 'keyge1', 'keyge2', 'count', 'keyge1__keyge', 'keyge2__keyge').filter(**v_where_cs).order_by('keyge1__keyge','keyge2__keyge')
            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Word not found'))
                sys.exit(2)
            if not QS:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Word not found'))
                sys.exit(2)
            return QS

        def get_wordmap(*args):
            if v_word != 'all':
                v_where_cs = {'word1__contains': v_word } # %like% melhorar esse processo
            else:
                v_where_cs = {}
            try:
                QS = WordMap.objects.filter(**v_where_cs).order_by('word1','word2')
            except ObjectDoesNotExist:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Word not found'))
                sys.exit(2)
            if not QS:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('  Word not found'))
                sys.exit(2)
            return QS


        # SHOW BLOCK
        if options['show']:
            v_table = str(options['show']).lower()

            if v_table == 'database':
                v_database = str(options['database']).lower()
                QS = get_database(v_database)
                self.stdout.write(self.style.HTTP_INFO(f'{f"ID":<5}{f"DATABASE":<15}{f"CATEGORY":<15}{f"DESCRIPTION":<50}{f"WEBSITE":<50}'))
                for qs in QS:                   
                    self.stdout.write(self.style.HTTP_SUCCESS(f'{f"{qs.id}":<5}{f"{qs.database}":<15}{f"{qs.category}":<15}{f"{qs.description}":<50}{f"{qs.website}":<50}')) 

            elif v_table == 'dataset':
                v_database =  str(options['database']).lower()
                v_dataset = str(options['dataset']).lower()
                QS = get_dataset(v_database, v_dataset)
                self.stdout.write(self.style.HTTP_INFO(f'{f"ID":<5}{f"DATABASE":<15}{f"DATASET":<15}{f"STATUS":<10}{f"DESCRIPTION":<50}')) 
                for qs in QS:
                    self.stdout.write(self.style.HTTP_SUCCESS(f'{f"{qs.id}":<5}{f"{qs.database}":<15}{f"{qs.dataset}":<15}{f"{qs.update_ds}":<10}{f"{qs.description}":<50}')) 
                  
            elif v_table == 'ds_column':
                v_dataset = str(options['dataset']).lower()
                QS = get_ds_column(v_dataset)
                v_ds = ''
                self.stdout.write(self.style.HTTP_INFO(f'{f"ID":<5}{f"DATASET":<15}{f"COL SEQ":<10}{f"COL NAME":<25}{f"STATUS":<10}{f"PREFIX":<10}'))
                for qs in QS:
                    if v_ds != str(qs.dataset):
                        print('')
                    if str(qs.pre_value) == 'none':
                        v_pre = ''
                    else:
                        v_pre = qs.pre_value
                    self.stdout.write(self.style.HTTP_SUCCESS(f'{f"{qs.id}":<5}{f"{qs.dataset}":<15}{f"{qs.column_number}":<10}{f"{qs.column_name}":<25}{f"{qs.status}":<10}{f"{v_pre}":<10}')) 
                    v_ds = str(qs.dataset)
            
            elif v_table == 'workflow':
                v_dataset = str(options['dataset']).lower()
                QS = get_workflow(v_dataset)
                self.stdout.write(self.style.HTTP_INFO(f'{f"DATASET":<15}{f"DT UPDATE":<25}{f"VERSION":<40}{f"SIZE":<15}{f"COLLECT":<10}{f"PREPARE":<10}{f"MAP":<10}{f"REDUCE":<10}'))
                for qs in QS:
                    if str(qs.last_update) != '':
                        v_upd = str(qs.last_update)[:19]
                    v_col = ''
                    v_pre = ''
                    v_red = ''
                    v_map = ''
                    if qs.chk_collect:
                        v_col = 'pass'
                    if qs.chk_prepare:
                        v_pre = 'pass'
                    if qs.chk_map:
                        v_map = 'pass'
                    if qs.chk_reduce:
                        v_red = 'pass'
                    self.stdout.write(self.style.HTTP_SUCCESS(f'{f"{qs.dataset}":<15}{f"{v_upd}":<25}{f"{qs.source_file_version}":<40}{f"{qs.source_file_size}":<15}{f"{v_col}":<10}{f"{v_pre}":<10}{f"{v_map}":<10}{f"{v_red}":<10}       ')) 
  
            elif v_table == 'keyge':
                v_group = str(options['group']).lower()
                v_category = str(options['category']).lower()
                QS = get_keyge(v_group, v_category)
                self.stdout.write(self.style.HTTP_INFO(f'{f"ID":<15}{f"GROUP":<15}{f"CATEGORY":<15}{f"KEYGE":<20}{f"DESCRIPTION":<50}')) 
                for qs in QS:         
                    self.stdout.write(self.style.HTTP_SUCCESS(f'{f"{qs.id}":<15}{f"{qs.group}":<15}{f"{qs.category}":<15}{f"{qs.keyge}":<20}{f"{qs.description}":<50}')) 

            elif v_table == 'category':
                QS = get_category()
                self.stdout.write(self.style.HTTP_INFO(f'{f"ID":<5}{f"CATEGORY":<15}{f"DESCRIPTION":<50}')) 
                for qs in QS:
                    self.stdout.write(self.style.HTTP_SUCCESS(f'{f"{qs.id}":<5}{f"{qs.category}":<15}{f"{qs.description}":<50}'))      

            elif v_table == 'group':
                QS = get_group()
                self.stdout.write(self.style.HTTP_INFO(f'{f"ID":<5}{f"GROUP":<15}{f"DESCRIPTION":<50}')) 
                for qs in QS:
                    self.stdout.write(self.style.HTTP_SUCCESS(f'{f"{qs.id}":<5}{f"{qs.group}":<15}{f"{qs.description}":<50}'))    
                 
            elif v_table == 'prefix':
                QS = get_prefix()
                self.stdout.write(self.style.HTTP_INFO(f'{f"pre_value":<15}')) 
                for qs in QS:                      
                    self.stdout.write(self.style.HTTP_SUCCESS(f'{f"{qs.pre_value}":<15}')) 

            elif v_table == 'key_word':
                v_word = str(options['word']).lower()
                QS = get_key_word(v_word)          
                self.stdout.write(self.style.HTTP_INFO(f'{f"STATUS":<10}{f"COMMUTE":<10}{f"KEYGE":<40}{f"WORD":<50}')) 
                for qs in QS:                   
                    self.stdout.write(self.style.HTTP_SUCCESS(f'{f"{qs.status}":<10}{f"{qs.commute}":<10}{f"{qs.keyge}":<40}{f"{qs.word}":<50}')) 
                              
            elif v_table == 'link_key':
                self.stdout.write(self.style.HTTP_NOT_FOUND('function not implemented'))
            
            elif v_table == 'wordmap':
                self.stdout.write(self.style.HTTP_NOT_FOUND('function not implemented'))
                
            else:
                self.stdout.write(self.style.HTTP_NOT_FOUND('Table not recognized in the system. Choose one of the options: '))
                self.stdout.write(self.style.HTTP_NOT_FOUND('   database | dataset | ds_column | workflow | keyge | category | group | prefix | key_word | link_key | wordmap'))



        # READ BLOCK
        if options['download']:

            v_path = str(options['path']).lower()
            v_table = str(options['download']).lower()

            if v_path == None:
                self.stdout.write(self.style.HTTP_BAD_REQUEST('Inform the path to download'))
                sys.exit(2)
            if not os.path.isdir(v_path) :
                self.stdout.write(self.style.HTTP_BAD_REQUEST('Path not found'))
                sys.exit(2)
            v_file = v_path + "/" + v_table + ".csv"

            if v_table == 'database':
                v_database = str(options['database']).lower()
                QS = get_database(v_database)
                DF = convert_to_dataframe(QS, fields=['database','description','website','category'], index=False)
                DF.to_csv(v_file, index=False)                  
                self.stdout.write(self.style.SUCCESS('File generated successfully'))

            if v_table == 'dataset':
                v_database =  str(options['database']).lower()
                v_dataset = str(options['dataset']).lower()
                QS = get_dataset(v_database, v_dataset)
                DF = convert_to_dataframe(QS, fields=['database','dataset','update_ds','source_path','source_web','source_compact','source_file_name','source_file_format','source_file_sep','source_file_skiprow','target_file_name','target_file_format','description'], index=False)
                DF.to_csv(v_file, index=False)                  
                self.stdout.write(self.style.SUCCESS('File generated successfully'))

            if v_table == 'ds_column':
                v_dataset = str(options['dataset']).lower()
                QS = get_ds_column(v_dataset)
                DF = convert_to_dataframe(QS, fields=['dataset','status','column_number','column_name','pre_value'], index=False)
                DF.to_csv(v_file, index=False)                  
                self.stdout.write(self.style.SUCCESS('File generated successfully'))

            if v_table == 'workflow':
                v_dataset = str(options['dataset']).lower()
                QS = get_workflow(v_dataset)
                DF = convert_to_dataframe(QS, fields=['dataset','last_update','source_file_version','source_file_size','target_file_size','chk_collect','chk_prepare','chk_map','chk_reduce'], index=False)
                DF.to_csv(v_file, index=False)                  
                self.stdout.write(self.style.SUCCESS('File generated successfully'))

            if v_table == 'keyge':
                v_group = str(options['group']).lower()
                v_category = str(options['category']).lower()
                QS = get_keyge(v_group, v_category)
                DF = convert_to_dataframe(QS, fields=['keyge','group','category','description'], index=False)
                DF.to_csv(v_file, index=False)                  
                self.stdout.write(self.style.SUCCESS('File generated successfully'))

            if v_table == 'category':
                QS = get_category()
                DF = convert_to_dataframe(QS, fields=['category','description'], index=False)
                DF.to_csv(v_file, index=False)                  
                self.stdout.write(self.style.SUCCESS('File generated successfully'))

            if v_table == 'group':
                QS = get_group()
                DF = convert_to_dataframe(QS, fields=['group','description'], index=False)
                DF.to_csv(v_file, index=False)                  
                self.stdout.write(self.style.SUCCESS('File generated successfully'))

            if v_table == 'prefix':
                QS = get_prefix()
                DF = convert_to_dataframe(QS, fields=['pre_value'], index=False)
                DF.to_csv(v_file, index=False)                  
                self.stdout.write(self.style.SUCCESS('File generated successfully'))

            if v_table == 'key_word':
                v_word = str(options['word']).lower()
                QS = get_key_word(v_word)
                DF = convert_to_dataframe(QS, fields=['status','commute','word','keyge'], index=False)
                DF.to_csv(v_file, index=False)                  
                self.stdout.write(self.style.SUCCESS('File generated successfully'))

            elif v_table == 'link_key':
                v_word = str(options['word']).lower()
                QS = get_link_key(v_word)
                DF = convert_to_dataframe(QS, fields=['ckey', 'dataset', 'keyge1', 'keyge1__keyge', 'keyge2', 'keyge2__keyge', 'count', ], index=False)
                DF.to_csv(v_file, index=False)                  
                self.stdout.write(self.style.SUCCESS('File generated successfully'))
            
            elif v_table == 'wordmap':
                v_word = str(options['word']).lower()
                QS = get_wordmap(v_word)
                DF = convert_to_dataframe(QS, fields=['cword','database','dataset','keyge1','keyge2','word1','word2','count'], index=False)
                DF.to_csv(v_file, index=False)                  
                self.stdout.write(self.style.SUCCESS('File generated successfully'))
            
            else:
                self.stdout.write(self.style.HTTP_NOT_FOUND('Table not recognized in the system. Choose one of the options: '))
                self.stdout.write(self.style.HTTP_NOT_FOUND('   database | dataset | ds_column | workflow | keyge | category | group | prefix | key_word | link_key | wordmap'))

        # WHITE BLOCK

        # Delete bock
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
     

     