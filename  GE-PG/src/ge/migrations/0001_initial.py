# Generated by Django 4.0.5 on 2022-06-10 21:02

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Blacklist',
            fields=[
                ('word', models.CharField(max_length=30, primary_key=True, serialize=False)),
            ],
        ),
        migrations.CreateModel(
            name='Category',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('category', models.CharField(max_length=20, unique=True)),
                ('description', models.CharField(max_length=200)),
            ],
            options={
                'verbose_name_plural': 'Categories',
            },
        ),
        migrations.CreateModel(
            name='Database',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('database', models.CharField(max_length=20, unique=True)),
                ('description', models.CharField(max_length=200)),
                ('category', models.CharField(max_length=20)),
                ('website', models.CharField(max_length=200)),
            ],
        ),
        migrations.CreateModel(
            name='Dataset',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('dataset', models.CharField(max_length=20, unique=True)),
                ('description', models.CharField(default='', max_length=200)),
                ('update_ds', models.BooleanField(default=True)),
                ('last_update', models.DateTimeField(blank=True, default='')),
                ('source_path', models.CharField(default='...', max_length=300)),
                ('source_web', models.BooleanField(default=True)),
                ('source_compact', models.BooleanField(default=False)),
                ('source_file_name', models.CharField(max_length=200)),
                ('source_file_format', models.CharField(max_length=200)),
                ('source_file_version', models.CharField(max_length=200)),
                ('source_file_size', models.IntegerField(default=0)),
                ('target_file_name', models.CharField(max_length=200)),
                ('target_file_format', models.CharField(max_length=200)),
                ('target_file_size', models.IntegerField(default=0)),
                ('database', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='ge.database')),
            ],
        ),
        migrations.CreateModel(
            name='Group',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('group', models.CharField(max_length=20, unique=True)),
                ('description', models.CharField(max_length=200)),
            ],
        ),
        migrations.CreateModel(
            name='Keyge',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('keyge', models.CharField(max_length=30, unique=True)),
                ('description', models.CharField(max_length=200)),
                ('category', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='ge.category')),
                ('group', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='ge.group')),
            ],
        ),
        migrations.CreateModel(
            name='LogsCollector',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('source_file_name', models.CharField(max_length=200)),
                ('date', models.DateTimeField(blank=True, default='')),
                ('dataset', models.CharField(max_length=200)),
                ('database', models.CharField(max_length=200)),
                ('version', models.CharField(max_length=200)),
                ('status', models.BooleanField(default=True)),
                ('size', models.IntegerField(default=0)),
            ],
        ),
        migrations.CreateModel(
            name='WordMap',
            fields=[
                ('cword', models.CharField(default='x', max_length=45, primary_key=True, serialize=False)),
                ('word1', models.CharField(max_length=20)),
                ('word2', models.CharField(max_length=20)),
                ('count', models.IntegerField(default=0)),
                ('database', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='ge.database')),
                ('dataset', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='ge.dataset')),
                ('keyge1', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='key_wordmap_1', to='ge.keyge')),
                ('keyge2', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='key_wordmap_2', to='ge.keyge')),
            ],
        ),
        migrations.CreateModel(
            name='KeyWord',
            fields=[
                ('word', models.CharField(max_length=30, primary_key=True, serialize=False)),
                ('keyge', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='ge.keyge')),
            ],
        ),
        migrations.CreateModel(
            name='KeyLink',
            fields=[
                ('ckey', models.CharField(default='x', max_length=20, primary_key=True, serialize=False)),
                ('count', models.IntegerField(default=0)),
                ('dataset', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='ge.dataset')),
                ('keyge1', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='key_keylinks_1', to='ge.keyge')),
                ('keyge2', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='key_keylinks_2', to='ge.keyge')),
            ],
        ),
    ]