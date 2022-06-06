from tkinter import Text
from turtle import update
from django.core.management.base import BaseCommand, CommandError
from ge.models import Database, Dataset


import os
from django.conf import settings
from django.http import HttpResponse, Http404, HttpRequest

"""

2. criar a rotina para o download
3. gravar o arquivo na pasta PSA
4. Atualizar os dados de controle
"""


class Command(BaseCommand):
    help = 'descrever sobre o modulo COLLECTOR'

    def add_arguments(self, parser):
        # Positional arguments
        parser.add_argument('ds_ids', nargs='+', type=int)

        # Named (optional) arguments
        parser.add_argument(
            '--process',
            action='store_true',
            help='Will process routine to download db files from internet',
        )

    def handle(self, *args, **options):

        # ler a tabela dataset com Update = true
        ds_queryset = Dataset.objects.filter(update=True)

        for ds in ds_queryset:
            self.stdout.write(self.style.SUCCESS('START:  "%s"' % ds.database))

            v_version = str(HttpRequest.GET(
                ds.source_path, stream=True).headers["etag"])

        # for ds_id in options['ds_ids']:
        #     try:
        #         db = Dataset.objects.get(id=ds_id)
        #         print(db)
        #     except Dataset.DoesNotExist:
        #         raise CommandError('DB "%s" does not exist' % ds_id)

        #     # poll.opened = False
        #     # poll.save()

        #     self.stdout.write(self.style.SUCCESS(
        #         'Successfully closed poll "%s"' % ds_id))

        #     # print(get_version())

        # if options['process']:
            """ 
            criar o codigo para realizar o download do arquivo e controle de versao

            1. selecionar os datase para download
            2. Loop em cada DS, ver o https://docs.djangoproject.com/en/4.0/topics/db/queries/
            3. criar a pasta
            4. Chack versao 
                 try:
                v_version = str(requests.get(
                    v_file_url, stream=True).headers["etag"])
            except:
                print("erro na extracao do etag")
                # Need better treatment here. Maybe: v_size_new = str(requests.get(file_url, stream=True).headers["Content-length"])
                v_version = "0"

            """

        # if options['delete']:
        #     poll.delete()
