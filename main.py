from src.create_table_raw import create_table_raw
from src.get_latest_date_from_table_VK import get_latest_date_from_table_VK
from src.import_from_VK_raw import *
from src.add_to_table import add_to_table
from src.create_table_field import create_table_field
from src.import_from_VK_slice import import_from_VK_slice



import pandas as pd
import yaml
from datetime import date, timedelta
import datetime
import os
import sys
import logging
import time

# Настройка логирования
file_log = logging.FileHandler('Log.log')
console_out = logging.StreamHandler()
logging.basicConfig(handlers=(file_log, console_out),
                    format='[%(asctime)s | %(module)s | %(funcName)s | %(levelname)s]: %(message)s',
                    datefmt='%m.%d.%Y %H:%M:%S',
                    level=logging.INFO)

# Считывание параметров конфигурации
with open(r'etc\config.yml') as file:
    config = yaml.load(file, Loader=yaml.FullLoader)

# Считывание параметров доступа к БД
with open(r'etc\credential_bd.yml') as file:
    credential_bd = yaml.load(file, Loader=yaml.FullLoader)

# Считывание параметров доступа к VK
with open(r'etc\credential_VK.yml') as file:
    credential_VK = yaml.load(file, Loader=yaml.FullLoader)

#
# *** Создание таблицы RAW
create_table_raw(credential_bd, credential_bd['table_name'])  # Создадие таблицы RAW

#
# *** Создание таблиц со срезами
fields_list = ['basic', 'sex', 'age', 'sex_age', 'cities']   # срезы
for field in fields_list:
    create_table_field(credential_bd, field)  # Создадие таблицы со срезом

# Проверяем файл конфигурации: период задается вручную?
if config['date_range']:
    DateFrom = input("Введите начальную дату в формате YYYY-MM-DD: ")
    DateTo = input("Введите конечную дату в формате YYYY-MM-DD: ")
    DateFrom = pd.to_datetime(DateFrom)
    DateTo = pd.to_datetime(DateTo)
else:
    # Ищем последнюю дату в текущей таблице
    latest_date = get_latest_date_from_table_VK(credential_bd)
    print(f"Последняя дата в текущей таблице - {latest_date}")

    # DateFrom: latest_date+1day
    DateFrom = pd.to_datetime(latest_date) + pd.DateOffset(1)
    # DateTo - вчерашняя дата
    DateTo = (date.today()- pd.DateOffset(1))

time_start = time.perf_counter()

# Проверяем, что начальная дата не превосходит начальную
if DateFrom > DateTo:
    print("В базе данных есть последние данные! Обновление не требуется! Выход из программы.")
    sys.exit()

#
# *** ИМПОРТИРУЕМ - ОБРАБАТЫВАЕМ - ЗАПИСЫВАЕМ RAW-данные
# Импортируем последние данные из VK - болғшаә таблиүа "vk_raw"

# Для формирования CSV-файла
now = datetime.datetime.now()
tmp = now.strftime("%Y-%m-%d__%H-%M")

# Здесь запишем цикл *****************
current_date = DateFrom
end_date = DateTo
delta = timedelta(days=1)
while current_date <= end_date:
    logging.info('#########################################################################')
    logging.info('#########################################################################')
    logging.info(f"{current_date} - ОБРАБАТЫВАЕМ ЭТУ ДАТУ")
    # Сколько ресурсов осталось...
    get_how_many_resurs(credential_VK)
    # ИМПОРТ в RAW-файл
    df = import_from_VK_raw(credential_VK, current_date.strftime('%Y-%m-%d'))

    # ИМПОРТ в файлы срезов (перечень - в словаре)
    dic_df = import_from_VK_slice(credential_VK, current_date.strftime('%Y-%m-%d'), fields_list)

    # Проверяем файл конфигурации: записывать в CSV-файл?
    if config['write_to_csv']:
        file_name = f"{config['path_csv']}{credential_bd['table_name']}_{tmp}.csv"
        # Параметры: 1-дописывать файл 2-Проверять - есть ли уже заголовки
        df.to_csv(file_name, sep=";",  decimal=',', mode='a', encoding='utf-8-sig', index=False, header=not os.path.exists(file_name))

        # ЗАПИСЬ ВСЕХ СРЕЗОВ - в несколько файлов
        for field, df in dic_df.items():
            file_name = f"{config['path_csv']}vk_campaign_{field}_{tmp}.csv"
            df.to_csv(file_name, sep=";", decimal=',', mode='a', encoding='utf-8-sig', index=False,
                      header=not os.path.exists(file_name))
    else:
        # Добавление данные из ДатаФрейма в таблицу БД
        add_to_table(credential_bd, credential_bd['table_name'].lower(), df)

        # ЗАПИСЬ ВСЕХ СРЕЗОВ - в несколько таблиц
        for field, df in dic_df.items():
            if df.shape[0] > 0: # Если датафрейм имеет строки
                add_to_table(credential_bd, f"vk_campaign_{field}".lower(), df)

    current_date += delta

time_finish = time.perf_counter()
print(f"Время работы программы:  {time_finish - time_start:0.1f} секунд")


