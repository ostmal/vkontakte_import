from sqlalchemy import create_engine

import logging

def add_to_table(credential_bd, table_name, df):
    """
    Добавление данные из ДатаФрейма в таблицу БД
    """
    # https://stackoverflow.com/questions/23103962/how-to-write-dataframe-to-postgres-table
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html
    # from sqlalchemy import create_engine
    # engine = create_engine('postgresql://username:password@host:port/mydatabase')
    engine_string = f"postgresql://{credential_bd['user']}:{credential_bd['password']}@{credential_bd['host']}:5432/{credential_bd['database']}"
    engine = create_engine(engine_string)
    # try:
    df.to_sql(table_name, engine, schema=credential_bd['schema'], if_exists='append', index=False)
