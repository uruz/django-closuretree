import uuid
from django.db import connection, models
from django.db.models import F


class CttManager(models.Manager):
    def bulk_create(self, *args, **kwargs):
        if not kwargs.pop('ctt_considered', False):
            raise RuntimeError(
                "Please don't forget to call closure_createlink() on model's "
                "manager for each created row and call bulk_create() "
                "with ctt_considered=True")

        return super(CttManager, self).bulk_create(*args, **kwargs)

    def closure_createlink(self, pk, parent_id):
        closure_table = self.model._closure_model._meta.db_table
        selects = ["SELECT 0, %s, %s"]
        query_args = [pk, pk]
        if parent_id:
            template = ("SELECT depth + 1, %s, parent_id "
                        "FROM {table} WHERE child_id = %s")
            selects.append(template.format(table=closure_table))
            query_args.extend([pk, parent_id])
        if isinstance(pk, uuid.UUID):
            query_args = map(unicode, query_args)

        insert_template = (
            "{insert} INTO {closure_table} (depth, child_id, parent_id) "
            "{selects_union}")

        if connection.vendor == 'sqlite':
            insert_operator = 'INSERT OR IGNORE'
        else:
            insert_operator = 'INSERT IGNORE'

        query_sql = insert_template.format(
            insert=insert_operator,
            closure_table=closure_table,
            selects_union=' UNION '.join(selects),
        )
        with connection.cursor() as cursor:
            cursor.execute(query_sql, query_args)

    def closure_update_links(self, instance, new_parent, old_parent):
        subtree_with_self = instance.get_descendants(include_self=True)
        subtree_without_self = instance.get_descendants().order_by('level')
        cached_subtree = [instance] + list(subtree_without_self)
        new_level = new_parent.level if new_parent is not None else 0
        old_level = old_parent.level if old_parent is not None else 0
        leveldiff = new_level - old_level
        subtree_without_self.update(level=F('level') + leveldiff)
        links = self.model._closure_model.objects.filter(child_id__in=subtree_with_self)
        links.delete()
        for item in cached_subtree:
            self.closure_createlink(item.pk, item._closure_parent_pk)
