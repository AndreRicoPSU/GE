# Generated by Django 4.0.5 on 2022-06-03 15:02

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('ge', '0004_alter_dataset_source_path'),
    ]

    operations = [
        migrations.AlterField(
            model_name='keyword',
            name='Key',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='ge.key'),
        ),
    ]