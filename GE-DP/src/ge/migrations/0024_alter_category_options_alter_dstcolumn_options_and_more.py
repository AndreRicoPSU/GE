# Generated by Django 4.0.5 on 2022-07-06 13:24

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('ge', '0023_alter_category_options_alter_database_options_and_more'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='category',
            options={'verbose_name_plural': 'Keyge - Category'},
        ),
        migrations.AlterModelOptions(
            name='dstcolumn',
            options={'verbose_name_plural': 'Dataset - Columns'},
        ),
        migrations.AlterModelOptions(
            name='group',
            options={'verbose_name_plural': 'Keyge - Group'},
        ),
        migrations.AlterModelOptions(
            name='keyhierarchy',
            options={'verbose_name_plural': 'Keyge - Hierarchy'},
        ),
        migrations.AlterModelOptions(
            name='keylink',
            options={'verbose_name_plural': 'Links - Keyge'},
        ),
        migrations.AlterModelOptions(
            name='keyword',
            options={'verbose_name_plural': 'Keyge - Word'},
        ),
        migrations.AlterModelOptions(
            name='prefixopc',
            options={'verbose_name_plural': 'Keyge - Prefix'},
        ),
        migrations.AlterModelOptions(
            name='wfcontrol',
            options={'verbose_name_plural': 'Dataset - Workflow'},
        ),
        migrations.AlterModelOptions(
            name='wordmap',
            options={'verbose_name_plural': 'Links - Word'},
        ),
    ]
