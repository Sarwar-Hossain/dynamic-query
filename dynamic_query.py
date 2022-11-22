import datetime
from django.db import connection
from psycopg2 import sql  # install psycopg2


def insert_data(self, table_name, data):
    column_list = list()
    value_list = list()

    # Convert the dictionary to lists
    for column, value in data.items():
        column_list.append(sql.Identifier(column))  # Convert to identifiers
        value_list.append(value)

    # Build the query, values will be inserted later
    query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING").format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(column_list),  # already sql.Identifier
        sql.SQL(', ').join([sql.Placeholder()] * len(value_list)))

    result = SqlHelper().insert(query, tuple(value_list))
    return result


def select_table_data(self, table_name, where_clause_column=None, where_clause_value=None):
    if where_clause_column is None and where_clause_value is None:
        query = sql.SQL("select * FROM {table}").format(
            table=sql.Identifier(table_name))
        result = SqlHelper().select(query, )
        return result
    else:
        query = sql.SQL("select * FROM {table} where {where_clause_column} = %s").format(
            table=sql.Identifier(table_name),
            where_clause_column=sql.Identifier(where_clause_column))
        values = (where_clause_value,)
        result = SqlHelper().select(query, values)
        return result


def update_table(self, table_name, data, where_clause_column, where_column_value):
    sql_query = sql.SQL("UPDATE {table} SET {data} WHERE {where_clause_column} = {where_column_value}").format(
        data=sql.SQL(', ').join(
            sql.Composed([sql.Identifier(k), sql.SQL(" = "), sql.Placeholder(k)]) for k in data.keys()
        ),
        table=sql.Identifier(table_name),
        where_clause_column=sql.Identifier(where_clause_column),
        where_column_value=sql.Placeholder('where_column_value')
    )

    data.update(where_column_value=where_column_value)
    result = SqlHelper().update(sql_query, data)
    return result


def delete_table_row(self, table_name, where_clause_column, where_clause_value):
    query = sql.SQL("delete FROM {table} where {where_clause_column} = %s").format(
        table=sql.Identifier(table_name),
        where_clause_column=sql.Identifier(where_clause_column))
    values = (where_clause_value,)
    result = SqlHelper().delete(query, values)
    return result
