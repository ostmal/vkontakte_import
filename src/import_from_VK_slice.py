import pandas as pd
import requests
import gspread
import time
import numpy as np
import logging
import datetime
import json
from time import sleep

def import_from_VK_slice(credential_VK,my_date, fields_list):
    """
    Импортируем данные из VK в ДатаФрейм - СРЕЗЫ
    На выходе: СЛОВАРЬ, ключи - из списка fields_list, значения - датафреймы со срезами
    """
    token = credential_VK['token']
    version = credential_VK['version']
    account_id = credential_VK['account_id']
    client_id = credential_VK['client_id']

    def getCampaigns():
        """
        ads.getCampaigns - список РК
        """
        time.sleep(0.5)  # задержка
        logging.info("ads.getCampaigns - выполняется.")
        req = requests.get('https://api.vk.com/method/ads.getCampaigns', params={
            'access_token': token,
            'v': version,
            'account_id': account_id,
            'client_id': client_id
        })
        data = req.json()['response']

        # Создаем датафрейм с РК
        df_campaigns = pd.DataFrame({
            'campaign_id': pd.Series(dtype='int'),  # идентификатор кампании
            'type': pd.Series(dtype='str'),  # тип кампании
            'name': pd.Series(dtype='str'),  # название кампании
        })
        for i in range(len(data)):
            campaign_id = data[i].setdefault('id')
            type = data[i].setdefault('type', "")
            name = data[i].setdefault('name', "")

            new_row = {
                'campaign_id': [campaign_id],
                'type': [type],
                'name': [name],
            }

            #     df_campaigns = df_campaigns.append(new_row, ignore_index=True)
            df_new_row = pd.DataFrame(new_row)
            df_campaigns = pd.concat([df_campaigns, df_new_row])

        return df_campaigns


    def getStatistics():
        """
        ads.getStatistics - Статистика за конкретный день по РК (просмотры, клики)
        """
        # Создадим Датафрейм
        df_stat = pd.DataFrame({
            'campaign_id': pd.Series(dtype='int'),
            'impressions': pd.Series(dtype='int'),
            'clicks': pd.Series(dtype='int'),
            'spent': pd.Series(dtype='float'),
            'conversion_count': pd.Series(dtype='int')
        })

        logging.info(f"{my_date} - по этой дате начинается выполняться запрос по каждой РК")
        for campaign_id in campaign_id_list:
            logging.info(f"{campaign_id} - id-РК. ads.getStatistics - выполняется.")
            time.sleep(
                0.5)  # Обязательно нужна задержка. Иначе VK-игнорирует запрос (Можно 2 раза в секунду)ю См:https://vk.com/dev/ads_limits
            req = requests.get('https://api.vk.com/method/ads.getStatistics', params={
                'access_token': token,
                'v': version,
                'account_id': account_id,
                'ids_type': 'campaign',
                'ids': campaign_id,
                #             'period': 'overall',   # данные за все время
                'period': 'day',
                'date_from': my_date,
                'date_to': my_date
            })

            try:
                data_stats = req.json()['response'][0]['stats'][0]  # Информация по одному дню
            except:
                logging.info("нет инфрмации по РК")
                continue  # нет инфрмации по объявлению

            impressions = data_stats.setdefault('impressions', 0)  # просмотры
            clicks = data_stats.setdefault('clicks', 0)  # клики
            spent = data_stats.setdefault('spent', 0)  # цена
            conversion_count = data_stats.setdefault('conversion_count', 0)  # конверсии

            new_row = {
                'campaign_id': [campaign_id],
                'impressions': [impressions],
                'clicks': [clicks],
                'spent': [spent],
                'conversion_count': [conversion_count],
            }

            #     df_stat = df_stat.append(new_row, ignore_index=True)
            df_new_row = pd.DataFrame(new_row)
            df_stat = pd.concat([df_stat, df_new_row])

        # Добавляем столбец с датой
        df_stat['date'] = pd.to_datetime(my_date)

        return df_stat

    def transform_basic(df_campaigns_stat):
        """
        Трансформируем таблицу basic
        """
        # Сортировать по "CampaignId"
        vk_campaign = df_campaigns_stat.sort_values("campaign_id")

        # Перевести поле "spent" в numeric
        vk_campaign['spent'] = pd.to_numeric(vk_campaign['spent'])

        # spent_1cl - цена за 1 клик
        # vk_campaign['spent_1cl'] = vk_campaign['spent'].div(vk_campaign['clicks'], fill_value=0).round(1).replace(            np.inf, 0)

        #  % кликов - CTR
        # vk_campaign['ctr_proc'] = vk_campaign['clicks'].div(vk_campaign['impressions'], fill_value=0).mul(100).round(            2).replace(np.inf, 0)

        # Переводим наименование колонок в нижний регистр
        vk_campaign.columns = vk_campaign.columns.str.lower()

        # Записываем значение log_datetime - текущая дата-время (логирование) ******
        vk_campaign['log_datetime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


        return vk_campaign

    def getDemographics():
        """
        ads.getDemographics - демографическая статистика по объявлениям
        на выходе сырая таблица, где статистика "зашита" в одной ячейке
        """
        # Создадим Датафрейм
        df_demographics = pd.DataFrame({
            'campaign_id': pd.Series(dtype='int'),
            'sex': pd.Series(dtype='str'),
            'age': pd.Series(dtype='str'),
            'sex_age ': pd.Series(dtype='str'),
            'cities': pd.Series(dtype='str'),
            'name': pd.Series(dtype='str'),
        })

        logging.info(f"{my_date} - по этой дате начинается выполняться запрос по каждому объявлению")
        for campaign_id in campaign_id_list:
            logging.info(f"{campaign_id} - id-РК     ads.getDemographics - выполняется.")
            time.sleep(
                0.5)  # Обязательно нужна задержка. Иначе VK-игнорирует запрос (Можно 2 раза в секунду)ю См:https://vk.com/dev/ads_limits
            req = requests.get('https://api.vk.com/method/ads.getDemographics', params={
                'access_token': token,
                'v': version,
                'account_id': account_id,
                'ids_type': 'campaign',
                'ids': campaign_id,
                #             'period': 'overall',   # данные за все время
                'period': 'day',
                'date_from': my_date,
                'date_to': my_date
            })

            try:
                data_stats = req.json()['response'][0]['stats'][0]  # Информация по одному дню
            except:
                logging.info("нет инфрмации по РК")
                continue  # нет инфрмации по объявлению

            sex = data_stats.setdefault('sex', "")  # статистика по полу
            age = data_stats.setdefault('age', "")  # статистика по возрасту
            sex_age = data_stats.setdefault('sex_age', "")  # статистика по полу+возрасту
            cities = data_stats.setdefault('cities', "")  # статистика по городам (коды)
            name = data_stats.setdefault('name', "")  # статистика по городам (наименования)

            new_row = {
                'campaign_id': [campaign_id],
                'sex': [sex],
                'age': [age],
                'sex_age': [sex_age],
                'cities': [cities],
                'name': [name],
            }

            #     df_demographics = df_demographics.append(new_row, ignore_index=True)
            df_new_row = pd.DataFrame(new_row)
            df_demographics = pd.concat([df_demographics, df_new_row])

        return df_demographics

    def get_statistics_from_cell(df):
        """
        https://stackoverflow.com/questions/35491274/split-a-pandas-column-of-lists-into-multiple-columns
        Функция получает на вход датафрейм с двумя колонками
        1-я колонка - id рекламной кампании
        2-я колонка - статистика по показателю. Статистика зашита в ячейке в виде списка словарей см:(https://dev.vk.com/method/ads.getDemographics)

        2-й параметр - "name_value" - ключ в словаре, под которым хранится значение (для городов - "name")

        На выходе - датафрейм с колонками:
        'campaign_id'
        [имя второй колонки входного df]
        'impressions_rate' - доля просмотров
        'clicks_rate' - доля кликов
        """
        # Список наименований столбцов
        orig_columns_names = df.columns.to_list()

        # Определяем ИМЯ КЛЮЧА, где хпнится значение (у городов это "name")
        name_value = 'value'
        if orig_columns_names[1] == 'cities':
            name_value = 'name'

        # Переименуем столбцы
        df = df.rename(columns={orig_columns_names[0]: 'x', orig_columns_names[1]: 'list'})

        # Индексируем
        df = df.set_index(['x'])

        # Раздвигаем - когда много значений в списке
        df = (pd.melt(df.list.apply(pd.Series).reset_index(),
                      id_vars=['x'],
                      value_name='list')
              .set_index(['x'])
              .drop('variable', axis=1)
              .dropna()
              .sort_index()
              )

        # Уберем индекс
        df = df.reset_index()

        # Создаем новые столбцы. Берем занчения из словаря
        df['impressions_rate'] = df['list'].apply(lambda x: x.setdefault('impressions_rate', 0))
        df['clicks_rate'] = df['list'].apply(lambda x: x.setdefault('clicks_rate', 0))
        df[name_value] = df['list'].apply(lambda x: x.setdefault(name_value, 0))

        # Поменяем порядок столбцов
        df = df.reindex(columns=['x', name_value, 'impressions_rate', 'clicks_rate', 'list'])

        # Переименуем столбцы
        df = df.rename(columns={'x': orig_columns_names[0], name_value: orig_columns_names[1]})

        # Удалим один столбец
        df = df.drop(columns='list')

        return df


    def creating_tables_sex_age_cities(field):
        """
        Создаем по каждому срезу (пол-возраст-города) таблицу
        """

        # Получаем "расплавленную таблицу"
        df_melt = get_statistics_from_cell(df_demographics[['campaign_id', field]])

        # Сливаем ДВЕ таблицы
        vk_campaign_field = df_campaigns_stat.merge(df_melt, how='right')

        # Вычисляем новые значения полей [impressions, clicks] в связи со значением пропорций
        vk_campaign_field['impressions'] = vk_campaign_field.impressions.mul(
            vk_campaign_field['impressions_rate']).round(0).astype(int)
        vk_campaign_field['clicks'] = vk_campaign_field.clicks.mul(vk_campaign_field['clicks_rate']).round(0).astype(
            int)

        # Удаляем лишние поля
        vk_campaign_field = vk_campaign_field.drop(columns=['impressions_rate', 'clicks_rate'])

        # Порядок столбцов - поменяем
        vk_campaign_field = vk_campaign_field.reindex(
            columns=['date', 'campaign_id', 'name', field, 'impressions', 'clicks'])

        # ****************** Трансформируем таблицу ************
        # Сортировать по "campaign_id" + field
        vk_campaign_field = vk_campaign_field.sort_values(["campaign_id", field])

        #  % кликов - CTR
        # vk_campaign_field['ctr_proc'] = vk_campaign_field['clicks'].div(vk_campaign_field['impressions'],fill_value=0).mul(100).round(2).replace(np.inf,0)

        # Переводим наименование колонок в нижний регистр
        vk_campaign_field.columns = vk_campaign_field.columns.str.lower()

        return vk_campaign_field


    # *********************************
    # *** ОСНОВНАЯ ЛОГИКА ***
    # *********************************

    # Инициализируем с ИТОГОВЫМИ датафреймами
    dic_df = {}

    # Все РК
    df_campaigns = getCampaigns()
    # Список всех РК
    campaign_id_list = df_campaigns.campaign_id.to_list()

    # Статистика по РК - просмотры, клики
    df_stat = getStatistics()

    """
    Создаем базовую таблицу по РК из двух
    """
    # Сливаем 2 таблицы
    df_campaigns_stat = df_stat.merge(df_campaigns[['campaign_id', 'name']], how='left')
    # Перестроим колонки в результирующей таблице
    df_campaigns_stat = df_campaigns_stat.reindex(columns=['date', 'campaign_id', 'name', 'impressions', 'clicks', 'spent', 'conversion_count'])

    # Первая таблица со срезом (фактически просто статистика по РК)
    dic_df['basic'] = transform_basic(df_campaigns_stat)


    """
    ВСЕ остальные таблицы со срезами
    """
    # Делаем запрос по демографии (там же города)
    df_demographics = getDemographics()

    # Дополняем словарь остальными таблицами (sex и т.д.)
    for field in fields_list[1:]:
        try:
            dic_df[field] = creating_tables_sex_age_cities(field)

            # Записываем значение log_datetime - текущая дата-время (логирование) ******
            dic_df[field]['log_datetime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        except:
            continue

    return dic_df
