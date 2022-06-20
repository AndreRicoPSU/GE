# Generated by Django 4.0.5 on 2022-06-13 17:48

import datetime
from django.db import migrations, models
from django.utils.timezone import utc


class Migration(migrations.Migration):

    dependencies = [
        ('ge', '0006_alter_dataset_last_update_alter_dstcolumn_pos_value_and_more'),
    ]

    operations = [
        migrations.RenameField(
            model_name='dstcolumn',
            old_name='database',
            new_name='dataset',
        ),
        migrations.AlterField(
            model_name='dataset',
            name='last_update',
            field=models.DateTimeField(default=datetime.datetime(2022, 6, 13, 17, 48, 39, 261227, tzinfo=utc), verbose_name='Last Update Dataset'),
        ),
    ]
