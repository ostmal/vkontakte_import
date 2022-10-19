import psycopg2
import logging

def create_table_field(credential_bd, field):
    """
    Создаем таблицу в PostgreSQL для срезов
    """
    # Соединение к PostgreSQL
    con = psycopg2.connect(
        database=credential_bd['database'],
        user=credential_bd['user'],
        password=credential_bd['password'],
        host=credential_bd['host']
    )

    cursor = con.cursor()
    tbl_name = 'vk_campaign_' + field

    # Отдельная структура для базового среза (просто статистика по РК)
    if field == 'basic':
        sql = f'''
            CREATE TABLE IF NOT EXISTS {credential_bd['schema']}.{tbl_name} (
                id SERIAL PRIMARY KEY,
                log_datetime timestamp,
                date  DATE,
                campaign_id  INT,
                name VARCHAR,
                impressions  numeric,
                clicks  numeric,
                spent  numeric,
                conversion_count  numeric
                );
        '''
    else:
        sql = f'''
            CREATE TABLE IF NOT EXISTS {credential_bd['schema']}.{tbl_name} (
                id SERIAL PRIMARY KEY,
                log_datetime timestamp,
                date  DATE,
                campaign_id  INT,
                name VARCHAR,
                {field} VARCHAR,
                impressions  numeric,
                clicks  numeric
                );
        '''

    cursor.execute(sql)
    con.commit()
    # Закрываем соединение
    con.close()