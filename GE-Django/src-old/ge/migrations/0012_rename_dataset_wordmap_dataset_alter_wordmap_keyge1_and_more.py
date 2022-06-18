# Generated by Django 4.0.5 on 2022-06-07 20:47

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('ge', '0011_rename_update_dataset_update_ds'),
    ]

    operations = [
        migrations.RenameField(
            model_name='wordmap',
            old_name='Dataset',
            new_name='dataset',
        ),
        migrations.AlterField(
            model_name='wordmap',
            name='keyge1',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='key_wordmap_1', to='ge.keyge'),
        ),
        migrations.AlterField(
            model_name='wordmap',
            name='keyge2',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='key_wordmap_2', to='ge.keyge'),
        ),
    ]