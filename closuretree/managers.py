from django.db import connection, models


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
