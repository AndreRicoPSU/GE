# Generated by Django 4.0.5 on 2022-06-08 12:10

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('ge', '0015_remove_keylink_ccat_remove_keylink_cgrp_and_more'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='blacklist',
            name='id',
        ),
        migrations.RemoveField(
            model_name='keyword',
            name='id',
        ),
        migrations.AlterField(
            model_name='blacklist',
            name='word',
            field=models.CharField(max_length=30, primary_key=True, serialize=False),
        ),
        migrations.AlterField(
            model_name='keyge',
            name='keyge',
            field=models.CharField(max_length=30, unique=True),
        ),
        migrations.AlterField(
            model_name='keylink',
            name='ckey',
            field=models.CharField(default='x', max_length=20, primary_key=True, serialize=False),
        ),
        migrations.AlterField(
            model_name='keyword',
            name='word',
            field=models.CharField(max_length=30, primary_key=True, serialize=False),
        ),
    ]