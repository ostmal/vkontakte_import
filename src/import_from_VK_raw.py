import logging

import pandas as pd
# from oauth2client.service_account import ServiceAccountCredentials
import requests
import gspread
import time
import numpy as np
import datetime
import json
from time import sleep
import sys


def import_from_VK_raw(credential_VK,my_date):
    """
    Импортируем данные из VK в ДатаФрейм - RAW
    """
    token = credential_VK['token']
    version = credential_VK['version']
    account_id = credential_VK['account_id']
    client_id = credential_VK['client_id']

    """
    ads.getAds - Считывание всех атрибутов объявлений
    """
    time.sleep(0.5)  # задержка
    logging.info("ads.getAds - выполняется.")
    req = requests.get('https://api.vk.com/method/ads.getAds', params={
        'access_token': token,
        'v': version,
        'include_deleted': 1,
        # Флаг, задающий необходимость вывода архивных объявлений (0 — выводить только активные объявления)
        'account_id': account_id,
        'client_id': client_id
    })

    return_error_code(req)
    try:
        data = req.json()['response']
    except:
        logging.error("ERROR! В ответе отсутствует ключ 'response'")

    # Создаем датафрейм с объявлениями
    df_ads = pd.DataFrame({
        'ad_id': pd.Series(dtype='int'),
        'campaign_id': pd.Series(dtype='int'),
        'ad_format': pd.Series(dtype='int'),  # формат объявления
        'cost_type': pd.Series(dtype='int'),  # тип оплаты
        'cpc': pd.Series(dtype='int'),  # (если cost_type = 0) цена за переход в копейках.
        'cpm': pd.Series(dtype='int'),  # (если cost_type = 1) цена за 1000 показов в копейках.
        'ocpm': pd.Series(dtype='int'),  # (если cost_type = 3) цена за действие для oCPM в копейках.
        'goal_type': pd.Series(dtype='int'),  # тип цели.
        'ad_platform': pd.Series(dtype='str'),
        # (если значение применимо к данному формату объявления) рекламные площадки, на которых будет показываться объявление
        'status': pd.Series(dtype='int'),  # статус объявления. Возможные значения
        'name': pd.Series(dtype='str'),  # название объявления.
        'approved': pd.Series(dtype='int'),  # статус модерации объявления. Возможные значения
    })

    for i in range(len(data)):
        ad_id = data[i].setdefault('id')
        campaign_id = data[i].setdefault('campaign_id')
        ad_format = data[i].setdefault('ad_format', np.nan)
        cost_type = data[i].setdefault('cost_type', np.nan)
        cpc = data[i].setdefault('cpc', 0)
        cpm = data[i].setdefault('cpm', 0)
        ocpm = data[i].setdefault('ocpm', 0)
        goal_type = data[i].setdefault('goal_type', np.nan)
        ad_platform = data[i].setdefault('ad_platform', "")
        status = data[i].setdefault('status', np.nan)
        name = data[i].setdefault('name', "")
        approved = data[i].setdefault('approved', np.nan)

        new_row = {
            'ad_id': [ad_id],
            'campaign_id': [campaign_id],
            'ad_format': [ad_format],
            'cost_type': [cost_type],
            'cpc': [cpc],
            'cpm': [cpm],
            'ocpm': [ocpm],
            'goal_type': [goal_type],
            'ad_platform': [ad_platform],
            'status': [status],
            'name': [name],
            'approved': [approved],
        }

        #     df_ads = df_ads.append(new_row, ignore_index=True)
        df_new_row = pd.DataFrame(new_row)
        df_ads = pd.concat([df_ads, df_new_row])

    # *** Делаю справочники для датафрейма
    ad_format = [1, 2, 3, 4, 5, 6, 9, 11, 12]
    ad_format_txt = [
        'изображение и текст',
        'большое изображение',
        'эксклюзивный формат',
        'продвижение сообществ или приложений, квадратное изображение',
        'приложение в новостной ленте (устаревший)',
        'мобильное приложение',
        'запись в сообществе',
        'адаптивный формат',
        'истории'
    ]
    df_ad_format = pd.DataFrame({'ad_format': ad_format, 'ad_format_txt': ad_format_txt})

    cost_type = [0, 1, 3]
    cost_type_txt = [
        'оплата за переходы',
        'оплата за показы',
        'оптимизированная оплата за показы'
    ]
    df_cost_type = pd.DataFrame({'cost_type': cost_type, 'cost_type_txt': cost_type_txt})

    goal_type = [1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24]
    goal_type_txt = [
        'показы', 'переходы', 'отправка заявок', 'вступления в сообщество',
        'добавление в корзину', 'добавление в список желаний', 'уточнение сведений', 'начало оформления заказа',
        'добавление платёжной информации', 'покупка', 'контакт', 'получение потенциального клиента',
        'запись на приём', 'регистрация', 'подача заявки', 'использование пробной версии', 'оформление подписки',
        'посещение страницы', 'просмотр контента', 'использование поиска', 'поиск местонахождения',
        'пожертвование средств',
        'конверсия'
    ]
    df_goal_type = pd.DataFrame({'goal_type': goal_type, 'goal_type_txt': goal_type_txt})

    ad_platform = ['0', '1', 'all', 'desktop', 'mobile']
    ad_platform_txt = [
        'ВКонтакте и сайты-партнёры',
        'только ВКонтакте',
        'все площадки',
        'полная версия сайта',
        'мобильный сайт и приложения'
    ]
    df_ad_platform = pd.DataFrame({'ad_platform': ad_platform, 'ad_platform_txt': ad_platform_txt})

    status = [0, 1, 2]
    status_txt = [
        'объявление остановлено',
        'объявление запущено',
        'объявление удалено'
    ]
    df_status = pd.DataFrame({'status': status, 'status_txt': status_txt})

    approved = [0, 1, 2, 3]
    approved_txt = [
        'объявление не проходило модерацию',
        'объявление ожидает модерации',
        'объявление одобрено',
        'объявление отклонено'
    ]
    df_approved = pd.DataFrame({'approved': approved, 'approved_txt': approved_txt})

    # *** Сливаю все справочники в датафрейм "df_ads"
    df_ads = df_ads \
        .merge(df_ad_format, how='left') \
        .merge(df_cost_type, how='left') \
        .merge(df_ad_platform, how='left') \
        .merge(df_status, how='left') \
        .merge(df_approved, how='left') \
        .merge(df_goal_type, how='left')


    """
    ads.getStatistics - Статистика по всем объявлениям за конкретный день (просмотры, клики)
    """
    # Создадим Датафрейм
    df_stat = pd.DataFrame({
        'ad_id': pd.Series(dtype='int'),
        'impressions': pd.Series(dtype='int'),
        'clicks': pd.Series(dtype='int'),
        'spent': pd.Series(dtype='float'),
        'conversion_count': pd.Series(dtype='int')
    })

    # Список всех Ad
    ad_id_list = df_ads.ad_id.to_list()

    logging.info(f"{my_date} - по этой дате начинается выполняться запрос по каждому объявлению")
    for ad_id in ad_id_list:
        logging.info(f"{ad_id} - id-объявления. ads.getStatistics - выполняется.")
        time.sleep(
            0.5)  # Обязательно нужна задержка. Иначе VK-игнорирует запрос (Можно 2 раза в секунду)ю См:https://vk.com/dev/ads_limits
        req = requests.get('https://api.vk.com/method/ads.getStatistics', params={
            'access_token': token,
            'v': version,
            'account_id': account_id,
            'ids_type': 'ad',
            'ids': ad_id,
            #             'period': 'overall',   # данные за все время
            'period': 'day',
            'date_from': my_date,
            'date_to': my_date
        })

        try:
            data_stats = req.json()['response'][0]['stats'][0]  # Информация по одному дню
        except:
            continue  # нет инфрмации по объявлению

        impressions = data_stats.setdefault('impressions', 0)  # просмотры
        clicks = data_stats.setdefault('clicks', 0)  # клики
        spent = data_stats.setdefault('spent', 0)  # цена
        conversion_count = data_stats.setdefault('conversion_count', 0)  # конверсии

        new_row = {
            'ad_id': [ad_id],
            'impressions': [impressions],
            'clicks': [clicks],
            'spent': [spent],
            'conversion_count': [conversion_count],
        }

        #     df_stat = df_stat.append(new_row, ignore_index=True)
        df_new_row = pd.DataFrame(new_row)
        df_stat = pd.concat([df_stat, df_new_row])

    df_stat['date'] = pd.to_datetime(my_date)


    """
    ads.getAdsLayout - описание внешнего вида рекламных объявлений
    """
    time.sleep(0.5)  # задержка
    logging.info("ads.getAdsLayout - выполняется.")
    req = requests.get('https://api.vk.com/method/ads.getAdsLayout', params={
        'access_token': token,
        'v': version,
        'include_deleted': 1,
        # Флаг, задающий необходимость вывода архивных объявлений (0 — выводить только активные объявления)
        'account_id': account_id,
        'client_id': client_id
    })
    data = req.json()['response']

    # Создаем датафрейм с объявлениями
    df_ads_layout = pd.DataFrame({
        'ad_id': pd.Series(dtype='int'),
        'title': pd.Series(dtype='str'),  # заголовок объявления
        'description': pd.Series(dtype='str'),  # описание объявления
        'link_url': pd.Series(dtype='str'),  # ссылка на рекламируемый объект

    })

    for i in range(len(data)):
        ad_id = data[i].setdefault('id')
        title = data[i].setdefault('title', "")
        description = data[i].setdefault('description', "")
        link_url = data[i].setdefault('link_url', "")

        new_row = {
            'ad_id': [ad_id],
            'title': [title],
            'description': [description],
            'link_url': [link_url],

        }

        #     df_ads_layout = df_ads_layout.append(new_row, ignore_index=True)
        df_new_row = pd.DataFrame(new_row)
        df_ads_layout = pd.concat([df_ads_layout, df_new_row])


    """
    ads.getAdsTargeting - параметры таргетинга рекламных объявлений
    """
    time.sleep(0.5)  # задержка
    logging.info("ads.getAdsTargeting - выполняется.")
    req = requests.get('https://api.vk.com/method/ads.getAdsTargeting', params={
        'access_token': token,
        'v': version,
        'include_deleted': 1,
        # Флаг, задающий необходимость вывода архивных объявлений (0 — выводить только активные объявления)
        'account_id': account_id,
        'client_id': client_id
    })
    data = req.json()['response']

    # Создаем датафрейм с объявлениями
    df_ads_targeting = pd.DataFrame({
        'ad_id': pd.Series(dtype='int'),
        'target_sex': pd.Series(dtype='int'),  # пол
        'target_age_from': pd.Series(dtype='int'),  # возраст (лет) - ОТ
        'target_age_to': pd.Series(dtype='int'),  # возраст (лет) - ДО
        'target_cities': pd.Series(dtype='str'),  # города
        'target_key_phrases': pd.Series(dtype='str'),  # ключевые фразы
    })

    for i in range(len(data)):
        ad_id = int(data[i].setdefault('id'))
        target_sex = float(data[i].setdefault('sex', np.nan))
        target_age_from = float(data[i].setdefault('age_from', np.nan))
        target_age_to = float(data[i].setdefault('age_to', np.nan))
        target_cities = data[i].setdefault('cities', "")
        target_key_phrases = data[i].setdefault('key_phrases', "")

        new_row = {
            'ad_id': [ad_id],
            'target_sex': [target_sex],
            'target_age_from': [target_age_from],
            'target_age_to': [target_age_to],
            'target_cities': [target_cities],
            'target_key_phrases': [target_key_phrases],
        }

        #     df_ads_targeting = df_ads_targeting.append(new_row, ignore_index=True)
        df_new_row = pd.DataFrame(new_row)
        df_ads_targeting = pd.concat([df_ads_targeting, df_new_row])

    # *** Делаю справочники
    target_sex = [0, 1, 2]
    target_sex_txt = ['any', 'f', 'm']
    df_sex = pd.DataFrame({'target_sex': target_sex, 'target_sex_txt': target_sex_txt})

    # Сливаю с таблицей
    # Cлева - таблица бОльшего размера (в принципе - они должны быть одинаковыми)
    if df_ads_targeting.shape[0] >= df_sex.shape[0]:
        df_ads_targeting = df_ads_targeting.merge(df_sex, how='left')
    else:
        df_ads_targeting = df_ads_targeting.merge(df_sex, how='right')
    # Уберу столбец "target_sex"
    df_ads_targeting = df_ads_targeting.drop('target_sex', axis='columns')


    """
    ads.getCampaigns - список кампаний
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

    # Создаем датафрейм с объявлениями
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


    """
    Объединяем ВСЕ датафреймы
    """
    logging.info(" Объединяем датафреймы, слева df_stat")
    df = df_stat \
        .merge(df_ads, how='left') \
        .merge(df_campaigns, how='left') \
        .merge(df_ads_layout, how='left') \
        .merge(df_ads_targeting, how='left')

    # *** Добавляем дату-время логирования
    df['log_datetime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    return df




def get_how_many_resurs(credential_VK):
    token = credential_VK['token']
    version = credential_VK['version']
    account_id = credential_VK['account_id']
    client_id = credential_VK['client_id']
    req = requests.get('https://api.vk.com/method/ads.getFloodStats', params={
        'access_token': token,
        'v': version,
        'account_id': account_id,
        'client_id': client_id
    })

    data = req.json()['response']
    refresh = data['refresh']
    logging.info(f"+++ {data['left']} - кол-во оставшихся методов")
    logging.info(f"+++ {refresh % 60}:{refresh // 60} - время до следующего обновления")


def return_error_code(resp):
    if resp.status_code != 200:
        logging.info(resp.status_code, " ------- " ,resp.text())
        raise Exception('Wrong response code')
    else:
        logging.info("200 - код с сервера")

