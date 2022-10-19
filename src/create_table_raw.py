import psycopg2
import logging

def create_table_raw(credential_bd, tbl_name):
    """
    Создаем таблицу в PostgreSQL
    """
    # Соединение к PostgreSQL
    con = psycopg2.connect(
        database=credential_bd['database'],
        user=credential_bd['user'],
        password=credential_bd['password'],
        host=credential_bd['host']
    )

    cursor = con.cursor()
    sql = f'''
        CREATE TABLE IF NOT EXISTS {credential_bd['schema']}.{tbl_name} (
            id SERIAL PRIMARY KEY,
            log_datetime timestamp,
            date  DATE,
            ad_id  INT,
            campaign_id  INT,
            ad_format  INT,
            cost_type  INT,
            cpc  INT,
            cpm  INT,
            ocpm  VARCHAR,
            goal_type  INT,
            ad_platform  VARCHAR,
            status  INT,
            name  VARCHAR,
            approved  INT,
            ad_format_txt  VARCHAR,
            cost_type_txt  VARCHAR,
            ad_platform_txt  VARCHAR,
            status_txt  VARCHAR,
            approved_txt  VARCHAR,
            goal_type_txt  VARCHAR,
            title  VARCHAR,
            description  VARCHAR,
            link_url  VARCHAR,
            type  VARCHAR,
            impressions  numeric,
            clicks  numeric,
            spent  numeric,
            conversion_count  numeric,
            target_age_from  INT,
            target_age_to  INT,
            target_cities  VARCHAR,
            target_key_phrases  VARCHAR,
            target_sex_txt  VARCHAR
            );
    '''

    cursor.execute(sql)
    con.commit()
    # Закрываем соединение
    con.close()