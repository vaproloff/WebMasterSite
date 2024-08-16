import asyncio
from datetime import datetime
import requests

from db.models import Url
from db.models import Metrics
from api.actions.urls import _add_new_urls
from api.actions.metrics_url import _add_new_metrics
from db.session import connect_db
from db.utils import get_last_update_date

date_format = "%Y-%m-%d"


async def add_data(data, last_update_date, async_session):
    for query in data['text_indicator_to_statistics']:
        query_name = query['text_indicator']['value']
        new_url = [Url(url=query_name)]
        metrics = []
        date = query['statistics'][0]["date"]
        data_add = {
            "date": date,
            "ctr": 0,
            "position": 0,
            "impression": 0,
            "demand": 0,
            "clicks": 0,
        }
        for el in query['statistics']:
            if date != el['date']:
                date = datetime.strptime(date, date_format)
                if date > last_update_date:
                    metrics.append(Metrics(
                        url=query_name,
                        date=date,
                        ctr=data_add['ctr'],
                        position=data_add['position'],
                        impression=data_add['impression'],
                        demand=data_add['demand'],
                        clicks=data_add['clicks']
                    ))
                date = el['date']
                data_add = {
                    "date": date,
                    "ctr": 0,
                    "position": 0,
                    "impression": 0,
                    "demand": 0,
                    "clicks": 0,
                }

            field = el["field"]
            if field == "IMPRESSIONS":
                data_add["impression"] = el["value"]
            elif field == "CLICKS":
                data_add["clicks"] = el["value"]
            elif field == "DEMAND":
                data_add["demand"] = el["value"]
            elif field == "CTR":
                data_add["ctr"] = el["value"]
            elif field == "POSITION":
                data_add["position"] = el["value"]
        await _add_new_urls(new_url, async_session)
        await _add_new_metrics(metrics, async_session)


async def get_data_by_page(page, last_update_date, URL, ACCESS_TOKEN, async_session):
    body = {
        "offset": page,
        "limit": 500,
        "device_type_indicator": "ALL",
        "text_indicator": "URL",
        "region_ids": [],
        "filters": {}
    }

    response = requests.post(URL, json=body, headers={'Authorization': f'OAuth {ACCESS_TOKEN}',
                                                      "Content-Type": "application/json; charset=UTF-8"})

    print(response.text[:100])
    data = response.json()

    await add_data(data, last_update_date, async_session)


async def get_all_data(request_session):
    config, group = request_session["config"], request_session["group"]
    DATABASE_NAME, ACCESS_TOKEN, USER_ID, HOST_ID, group = (config['database_name'],
                                                                  config['access_token'],
                                                                  config['user_id'],
                                                                  config['host_id'],
                                                                  group['name'])

    async_session = await connect_db(DATABASE_NAME)

    # Формируем URL для запроса мониторинга поисковых запросов
    URL = f"https://api.webmaster.yandex.net/v4/user/{USER_ID}/hosts/{HOST_ID}/query-analytics/list"

    body = {
        "offset": 0,
        "limit": 500,
        "device_type_indicator": "ALL",
        "text_indicator": "URL",
        "region_ids": [],
        "filters": {}
    }

    response = requests.post(URL, json=body, headers={'Authorization': f'OAuth {ACCESS_TOKEN}',
                                                      "Content-Type": "application/json; charset=UTF-8"})

    data = response.json()
    count = data["count"]
    print(count)
    last_update_date = await get_last_update_date(async_session, Metrics)
    print("last update date:", last_update_date)
    if not last_update_date:
        last_update_date = datetime.strptime("1900-01-01", date_format)
    await add_data(data, last_update_date, async_session)
    for offset in range(500, count, 500):
        print(f"[INFO] PAGE{offset} DONE!")
        await get_data_by_page(offset, last_update_date, URL, ACCESS_TOKEN, async_session)


if __name__ == '__main__':
    asyncio.run(get_all_data())
