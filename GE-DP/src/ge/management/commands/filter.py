from ge.models import Category, DSTColumn, Group, KeyHierarchy, KeyLink, Keyge, Dataset, KeyWord, Database, LogsCollector, PrefixOpc, WFControl, WordMap
from django.core.management.base import BaseCommand
import pandas as pd
from ge.models import Dataset, DSTColumn, WFControl
from django.core.exceptions import ObjectDoesNotExist

""" 
Get data from Igem Database to csv file

Subprocess:
    1. 
"""

class Command(BaseCommand):
    help = 'Get data from Igem Database'

    def add_arguments(self, parser):

        # Named (optional) arguments

        parser.add_argument(
            '--dataset',
            action='store_true',
            help='get data on tables',
        )

        parser.add_argument(
            '--database',
            action='store_true',
            help='get data on tables',
        )
    
        parser.add_argument(
            '--keyge',
            action='store_true',
            help='get data on tables',
        )
    
        parser.add_argument(
            '--keylink',
            action='store_true',
            help='get data on tables',
        )

        parser.add_argument(
            '--group',
            action='store_true',
            help='get data on tables',
        )

        parser.add_argument(
            '--category',
            action='store_true',
            help='get data on tables',
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
        


        if options['dataset']:
            qs = Dataset.objects.all()
            dataset = convert_to_dataframe(qs, fields=['id','dataset'], index=False)
            print(dataset)
            dataset.to_csv('loader/output_from_filter.csv')

        if options['database']:
            qs = Database.objects.all()
            dataset = convert_to_dataframe(qs, fields=['id','database'], index=False)
            print(dataset)
            dataset.to_csv('loader/output_from_filter.csv')

        if options['keyge']:
            qs = Keyge.objects.all()
            dataset = convert_to_dataframe(qs, fields=['id','keyge'], index=False)
            print(dataset)
            dataset.to_csv('loader/output_from_filter.csv')

        if options['keylink']:
            qs = KeyLink.objects.all()
            dataset = convert_to_dataframe(qs, fields=['keyge1','keyge2'], index=False)
            print(dataset)
            dataset.to_csv('loader/output_from_filter.csv')

        if options['group']:
            qs = Group.objects.all()
            dataset = convert_to_dataframe(qs)
            print(dataset)
            # dataset.to_csv('loader/output_from_filter.csv')

        if options['category']:
            qs = Category.objects.all()
            dataset = convert_to_dataframe(qs)
            print(dataset)
            # dataset.to_csv('loader/output_from_filter.csv')