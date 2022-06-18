# Generated by Django 4.0.5 on 2022-06-10 17:15

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('ge', '0018_dataset_description'),
    ]

    operations = [
        migrations.AddField(
            model_name='dataset',
            name='source_local',
            field=models.CharField(blank=True, max_length=200),
        ),
        migrations.AddField(
            model_name='dataset',
            name='source_local_flag',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='dataset',
            name='source_path',
            field=models.URLField(blank=True, max_length=300),
        ),
    ]