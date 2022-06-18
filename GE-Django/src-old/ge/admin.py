from django.contrib import admin

from .models import Database, Dataset, Group, Category, Blacklist, Keyge, KeyWord, WordMap, KeyLink


# # class ChoiceInline(admin.StackedInline):
# class ChoiceInline(admin.TabularInline):
#     model = Choice
#     extra = 3


# class QuestionAdmin(admin.ModelAdmin):
#     fieldsets = [
#         (None,               {'fields': ['question_text']}),
#         ('Date information', {'fields': [
#          'pub_date'], 'classes': ['collapse']}),
#     ]
#     inlines = [ChoiceInline]
#     list_display = ('question_text', 'pub_date', 'was_published_recently')
#     list_filter = ['pub_date']


class DatabaseAdmin(admin.ModelAdmin):
    model = Database
    list_display = ('database', 'category', 'description')
    list_filter = ['category']


class DatasetAdmin(admin.ModelAdmin):
    model = Dataset
    list_display = ('database', 'dataset', 'update_ds', 'last_update')
    # list_display = ('database', 'dataset', 'update_ds')
    list_filter = ['database', 'update_ds']

class WordMapAdmin(admin.ModelAdmin):
    model = WordMap
    list_display = ('dataset', 'word1', 'word2', 'count')
    list_filter = ['dataset', 'word1', 'word2']


# admin.site.register(Question, QuestionAdmin)
admin.site.register(Database, DatabaseAdmin)
admin.site.register(Dataset, DatasetAdmin)
admin.site.register(Group)
admin.site.register(Category)
admin.site.register(Blacklist)
admin.site.register(Keyge)
admin.site.register(KeyWord)
admin.site.register(WordMap, WordMapAdmin)
admin.site.register(KeyLink)