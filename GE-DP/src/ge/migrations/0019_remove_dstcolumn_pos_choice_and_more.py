# Generated by Django 4.0.5 on 2022-06-22 17:54

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('ge', '0018_delete_blacklist_alter_wfcontrol_last_update'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='dstcolumn',
            name='pos_choice',
        ),
        migrations.RemoveField(
            model_name='dstcolumn',
            name='pos_value',
        ),
        migrations.AlterField(
            model_name='dataset',
            name='update_ds',
            field=models.BooleanField(default=True, verbose_name='Activate'),
        ),
        migrations.CreateModel(
            name='KeyHierarchy',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('keyge', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='key_child', to='ge.keyge', verbose_name='Keyge ID')),
                ('keyge_parent', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='key_parent', to='ge.keyge', verbose_name='Keyge Parent ID')),
            ],
        ),
    ]
