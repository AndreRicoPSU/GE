# Generated by Django 4.0.5 on 2022-08-11 22:21

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('ge', '0026_alter_dataset_target_file_keep_alter_keyword_word'),
    ]

    operations = [
        migrations.AlterField(
            model_name='wfcontrol',
            name='source_file_size',
            field=models.BigIntegerField(default=0),
        ),
        migrations.AlterField(
            model_name='wfcontrol',
            name='source_file_version',
            field=models.CharField(max_length=500),
        ),
        migrations.AlterField(
            model_name='wfcontrol',
            name='target_file_size',
            field=models.BigIntegerField(default=0),
        ),
    ]