from pyexpat import model
from django.db import models


class Database(models.Model):
    database = models.CharField(max_length=20, unique=True)
    description = models.CharField(max_length=200)
    category = models.CharField(max_length=20)
    website = models.CharField(max_length=200)

    def __str__(self):
        return self.database


class Dataset(models.Model):
    dataset = models.CharField(max_length=20, unique=True)
    database = models.ForeignKey(Database, on_delete=models.CASCADE)
    description = models.CharField(max_length=200, default="")
    update_ds = models.BooleanField(default=True, verbose_name="Activate")
    source_path = models.CharField(max_length=300, default="") 
    source_web = models.BooleanField(default=True, verbose_name='Source path from Internet')
    source_compact = models.BooleanField(default=False)
    source_file_name = models.CharField(max_length=200)
    source_file_format = models.CharField(max_length=200)
    source_file_sep = models.CharField(max_length=3, default = ",")
    source_file_skiprow = models.IntegerField(default=0) 
    target_file_name = models.CharField(max_length=200)
    target_file_format = models.CharField(max_length=200)

    def __str__(self):
        return self.dataset

class WFControl(models.Model):
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    last_update = models.DateTimeField(verbose_name="Last Update Dataset")
    source_file_version = models.CharField(max_length=200)
    source_file_size = models.IntegerField(default=0)
    target_file_size = models.IntegerField(default=0)
    chk_collect = models.BooleanField(default=False, verbose_name='Collect Processed')
    chk_prepare = models.BooleanField(default=False, verbose_name='Prepare Processed')
    chk_commute = models.BooleanField(default=False, verbose_name='Commute Processed')
    chk_mapreduce = models.BooleanField(default=False, verbose_name='MapReduce Processed')


class LogsCollector(models.Model):
    source_file_name = models.CharField(max_length=200)
    date = models.DateTimeField(auto_now=False, auto_now_add=False, blank=True, default='')
    dataset = models.CharField(max_length=200)
    database = models.CharField(max_length=200)
    version = models.CharField(max_length=200)
    status = models.BooleanField(default=True)
    size = models.IntegerField(default=0)


class Group(models.Model):
    group = models.CharField(max_length=20, unique=True)
    description = models.CharField(max_length=200)

    def __str__(self):
        return self.group


class Category(models.Model):
    category = models.CharField(max_length=20, unique=True)
    description = models.CharField(max_length=200)

    def __str__(self):
        return self.category

    class Meta:
        verbose_name_plural = "Categories"


class Keyge(models.Model):
    keyge = models.CharField(max_length=40, unique=True)
    description = models.CharField(max_length=400)
    group = models.ForeignKey(Group, on_delete=models.CASCADE)
    category = models.ForeignKey(Category, on_delete=models.CASCADE)

    def __str__(self):
        return self.keyge


class KeyWord(models.Model):
    word = models.CharField(max_length=100, primary_key=True)
    keyge = models.ForeignKey(Keyge, on_delete=models.CASCADE)
    status = models.BooleanField(default=False, verbose_name='Active?')
    commute = models.BooleanField(default=False, verbose_name='Commute?')

    def __str__(self):
        linker = str(self.keyge) + " - " + str(self.word)
        return linker


class WordMap(models.Model):
    cword = models.CharField(max_length=15, primary_key=True)
    database = models.ForeignKey(Database, on_delete=models.CASCADE)
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    keyge1 = models.ForeignKey(Keyge, related_name='key_wordmap_1', blank=True, null=True,
                               on_delete=models.CASCADE)
    keyge2 = models.ForeignKey(Keyge, related_name='key_wordmap_2', blank=True, null=True,
                               on_delete=models.CASCADE)
    word1 = models.CharField(max_length=100)
    word2 = models.CharField(max_length=100)
    count = models.IntegerField(default=0)

    def __str__(self):
        linker = str(self.word1) + " - " + str(self.word2)
        return linker


class KeyLink(models.Model):
    ckey = models.CharField(max_length=15, primary_key=True)
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    keyge1 = models.ForeignKey(Keyge, related_name='key_keylinks_1',
                               on_delete=models.CASCADE)
    keyge2 = models.ForeignKey(Keyge, related_name='key_keylinks_2',
                               on_delete=models.CASCADE)
    count = models.IntegerField(default=0)


class DSTColumn(models.Model):
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    status = models.BooleanField(default=False, verbose_name='Active?')
    column_number = models.IntegerField(default=0, verbose_name='Column Sequence')
    column_name = models.CharField(max_length=40, blank=True, verbose_name='Column Name')
    pre_choice = models.BooleanField(default=False, verbose_name='Prefix?')
    pre_value = models.CharField(max_length=5, blank=True, verbose_name='Value Prefix')

class KeyHierarchy(models.Model):
    keyge = models.ForeignKey(Keyge, related_name='key_child', on_delete=models.CASCADE, verbose_name='Keyge ID')
    keyge_parent = models.ForeignKey(Keyge, related_name='key_parent', on_delete=models.CASCADE, verbose_name='Keyge Parent ID')