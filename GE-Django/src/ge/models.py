from django.db import models
from django.utils import timezone
from django.contrib import admin
import datetime


class Question(models.Model):
    question_text = models.CharField(max_length=200)
    pub_date = models.DateTimeField('date published')

    def __str__(self):
        return self.question_text

    @admin.display(
        boolean=True,
        ordering='pub_date',
        description='Published recently?',
    )
    def was_published_recently(self):
        now = timezone.now()
        return now - datetime.timedelta(days=1) <= self.pub_date <= now
        # return self.pub_date >= timezone.now() - datetime.timedelta(days=1)


class Choice(models.Model):
    question = models.ForeignKey(Question, on_delete=models.CASCADE)
    choice_text = models.CharField(max_length=200)
    votes = models.IntegerField(default=0)

    def __str__(self):
        return self.choice_text


# Connectors Parameters to Extract Data

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
    update = models.BooleanField(default=True)
    last_update = models.DateTimeField(
        auto_now=False, auto_now_add=False, blank=True)
    source_path = models.URLField(max_length=300)
    source_compact = models.BooleanField(default=False)
    source_file_name = models.CharField(max_length=200)
    source_file_format = models.CharField(max_length=200)
    source_file_version = models.CharField(max_length=200)
    source_file_size = models.IntegerField(default=0)
    target_file_name = models.CharField(max_length=200)
    target_file_format = models.CharField(max_length=200)
    target_file_size = models.IntegerField(default=0)

    def __str__(self):
        return self.dataset

## Key and Words


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


class Blacklist(models.Model):
    word = models.CharField(max_length=20, unique=True)

    def __str__(self):
        return self.word


class Keyge(models.Model):
    keyge = models.CharField(max_length=20, unique=True)
    description = models.CharField(max_length=200)
    group = models.ForeignKey(Group, on_delete=models.CASCADE)
    category = models.ForeignKey(Category, on_delete=models.CASCADE)

    def __str__(self):
        return self.keyge


class KeyWord(models.Model):
    word = models.CharField(max_length=20, unique=True)
    keyge = models.OneToOneField(Keyge, on_delete=models.CASCADE)

    def __str__(self):
        linker = str(self.keyge) + " - " + str(self.word)
        return linker


class WordMap(models.Model):
    database = models.ForeignKey(Database, on_delete=models.CASCADE)
    Dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    keyge1 = models.ForeignKey(Keyge, related_name='key_wordmap_1',
                               on_delete=models.CASCADE)
    keyge2 = models.ForeignKey(Keyge, related_name='key_wordmap_2',
                               on_delete=models.CASCADE)
    word1 = models.CharField(max_length=20)
    word2 = models.CharField(max_length=20)
    count = models.IntegerField(default=0)


class KeyLink(models.Model):
    ckey = models.CharField(max_length=20)
    database = models.ForeignKey(Database, on_delete=models.CASCADE)
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    cgrp = models.CharField(max_length=20)
    ccat = models.CharField(max_length=20)
    keyge1 = models.ForeignKey(Keyge, related_name='key_keylinks_1',
                               on_delete=models.CASCADE)
    keyge2 = models.ForeignKey(Keyge, related_name='key_keylinks_2',
                               on_delete=models.CASCADE)
    count = models.IntegerField(default=0)
