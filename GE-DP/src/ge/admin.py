from django.contrib import admin

from .models import DSTColumn, Database, Dataset, Group, Category, Blacklist, Keyge, KeyWord, WordMap, KeyLink, WFControl

class DatabaseAdmin(admin.ModelAdmin):
    model = Database
    list_display = ('database', 'category', 'description')
    list_filter = ['category']


class ChoiceDSTColumn(admin.TabularInline):
    model = DSTColumn
    fieldsets = [
        ('Transformation Columns',              {'fields': ['column_number','column_name','status','pre_choice','pre_value','pos_choice','pos_value'],'classes': ['collapse']})]
    extra = 1


class DatasetAdmin(admin.ModelAdmin):
    fieldsets = [
        (None,              {'fields': ['database','dataset','description','update_ds']}),
        # ('Log',             {'fields': [last_update','source_file_size','target_file_size','source_file_version'],'classes': ['collapse']}),
        ('Attributes',      {'fields': ['source_web','source_path','source_file_name','source_file_format','source_file_sep','source_file_skiprow','source_compact','target_file_name','target_file_format'],'classes': ['collapse']}), 
    ]
    
    inlines = [ChoiceDSTColumn]

    # model = Dataset
    list_display = ('database', 'dataset', 'update_ds', 'description')
    list_filter = ['update_ds','database']




class WordMapAdmin(admin.ModelAdmin):
    model = WordMap
    list_display = ('dataset', 'word1', 'word2', 'count')
    list_filter = ['dataset']

class KeygeAdmin(admin.ModelAdmin):
    model = Keyge
    list_display = ('id','keyge','get_group','get_category')
    list_filter = ['group_id','category_id']

    @admin.display(description='Group Name', ordering='group__group')
    def get_group(self, obj):
        return obj.group.group
    
    @admin.display(description='Category Name', ordering='category__category')
    def get_category(self, obj):
        return obj.category.category

class KeyLinkAdmin(admin.ModelAdmin):
    model = KeyLink
    list_display = ('dataset','keyge1','keyge2','count')

class DSTCAdmin(admin.ModelAdmin):
    model = DSTColumn

class KeyWordAdmin(admin.ModelAdmin):
    model = KeyWord
    list_display = ('keyge_id','get_keyge','word','status','commute')

    @admin.display(description='Keyge', ordering='keyge__keyge')
    def get_keyge(self, obj):
        return obj.keyge.keyge


# admin.site.register(Question, QuestionAdmin)
admin.site.register(Database, DatabaseAdmin)
admin.site.register(Dataset, DatasetAdmin)
admin.site.register(Group)
admin.site.register(Category)
admin.site.register(Blacklist)
admin.site.register(WFControl)
admin.site.register(Keyge, KeygeAdmin)
admin.site.register(KeyWord, KeyWordAdmin)
admin.site.register(WordMap, WordMapAdmin)
admin.site.register(KeyLink, KeyLinkAdmin)
admin.site.register(DSTColumn, DSTCAdmin)
