import requests
import asyncio
from db.session import async_session
from db.models import Url
from db.models import Metrics
from api.actions.urls import _add_new_urls
from api.actions.metrics import _add_new_metrics
from sqlalchemy.exc import IntegrityError

ACCESS_TOKEN = "y0_AgAEA7qkeLqBAAuw7AAAAAEDGzynAABr7pqZPg9NEb5O0OacK2wWzfFG2A"
USER_ID = "1130000065018497"
HOST_ID = "https:dn.ru:443"

url = f"https://api.webmaster.yandex.net/v4/user/{USER_ID}/hosts/{HOST_ID}/query-analytics/list"


# TODO: Change date add: now - datetime.now, must be date from json data

async def add_data(data):
    for el in data["text_indicator_to_statistics"]:
        url = el["text_indicator"]["value"]
        new_url = [Url(url=url)]
        metrics = []
        for i in range(1, 15):
            metric_date = el["statistics"][(i - 1) * 4:(4 * i)]

            if len(metric_date) == 0:
                continue

            try:
                add_impression = metric_date[0]["value"]
            except IndexError:
                add_impression = 0

            try:
                add_position = metric_date[1]["value"]
            except IndexError:
                add_position = 0

            try:
                add_clicks = metric_date[2]["value"]
            except IndexError:
                add_clicks = 0

            try:
                add_ctr = metric_date[3]["value"]
            except IndexError:
                add_ctr = 0

            metrics.append(Metrics(
                url=url,
                impression=add_impression,
                position=add_position,
                clicks=add_clicks,
                ctr=add_ctr,
            )
            )
        try:
            await _add_new_urls(new_url, async_session)
        except IntegrityError as e:
            print(e)
        await _add_new_metrics(metrics, async_session)


async def get_data_by_page(page):
    body = {
        "offset": page,
        "limit": 500,
        "device_type_indicator": "ALL",
        "text_indicator": "URL",
        "region_ids": [],
        "filters": {}
    }

    response = requests.post(url, json=body, headers={'Authorization': f'OAuth {ACCESS_TOKEN}',
                                                      "Content-Type": "application/json; charset=UTF-8"})

    print(response.text[:100])
    data = response.json()

    await add_data(data)


async def get_all_data():
    body = {
        "offset": 0,
        "limit": 500,
        "device_type_indicator": "ALL",
        "text_indicator": "URL",
        "region_ids": [],
        "filters": {}
    }

    response = requests.post(url, json=body, headers={'Authorization': f'OAuth {ACCESS_TOKEN}',
                                                      "Content-Type": "application/json; charset=UTF-8"})

    print(response.text[:100])
    data = response.json()
    count = data["count"]
    await add_data(data)
    functions = []
    for offset in range(500, count, 500):
        print(f"[INFO] PAGE{offset} DONE!")
        await get_data_by_page(offset)

    # await asyncio.gather(*functions)


if __name__ == '__main__':
    asyncio.run(get_all_data())
