# Generated by Django 4.0.5 on 2022-06-08 17:58

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('ge', '0017_alter_keyword_keyge'),
    ]

    operations = [
        migrations.AddField(
            model_name='dataset',
            name='description',
            field=models.CharField(default='description...', max_length=200),
        ),
    ]