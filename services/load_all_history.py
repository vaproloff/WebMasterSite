import asyncio
from datetime import datetime, timedelta

import requests

import config
from api.actions.indicators import _add_new_indicators
from db.models import QueryIndicator, UpdateLogsIndicator

from db.session import async_session
from db.utils import get_last_update_date, add_last_update_date

ACCESS_TOKEN = f"{config.ACCESS_TOKEN}"
USER_ID = f"{config.USER_ID}"
HOST_ID = f"{config.HOST_ID}"

date_format = "%Y-%m-%d"


def create_url(date_from):
    return (f"https://api.webmaster.yandex.net/v4/user/{USER_ID}/hosts/{HOST_ID}/search-queries/all/history?"
            f"query_indicator=TOTAL_SHOWS&"
            f"query_indicator=TOTAL_CLICKS&"
            f"query_indicator=AVG_SHOW_POSITION&"
            f"query_indicator=AVG_CLICK_POSITION&"
            f"date_from={date_from}")


async def get_response(async_session):
    last_update_date = await get_last_update_date(async_session, UpdateLogsIndicator)
    if not last_update_date:
        last_update_date = (datetime.now() - timedelta(days=60))
    print(last_update_date.date())
    print("Начало выгрузки")
    response = requests.get(create_url(last_update_date.date()), headers={'Authorization': f'OAuth {ACCESS_TOKEN}',
                                                                          "Content-Type": "application/json; charset=UTF-8"})

    return response


async def add_data(response: requests.models.Response):
    indicators = response.json()["indicators"]

    data_for_db = list()
    data_for_total_ctr = dict()
    for indicator in indicators:
        for element in indicators[indicator]:
            date = datetime.strptime(element["date"].split("T")[0], date_format)
            data_for_db.append(QueryIndicator(indicator=indicator,
                                              value=round(element["value"], 1),
                                              date=date))
            if date not in data_for_total_ctr:
                data_for_total_ctr[date] = [0, 0]
            if indicator == "TOTAL_CLICKS":
                data_for_total_ctr[date][0] = element["value"]
            elif indicator == "TOTAL_SHOWS":
                data_for_total_ctr[date][1] = element["value"]
    for key, value in data_for_total_ctr.items():
        value = round(value[0] * 100 / value[1], 2) if value[1] != 0 else 0
        data_for_db.append(QueryIndicator(
            indicator="TOTAL_CTR",
            value=value,
            date=key
        )
        )

    await _add_new_indicators(data_for_db, async_session)

    return data_for_db


async def main():
    response = await get_response(async_session)
    await add_data(response)
    date = (datetime.now().date() - timedelta(days=2)).strftime(date_format)
    date = datetime.strptime(date, date_format).date()
    await add_last_update_date(async_session, UpdateLogsIndicator, date)
    print("Выгрузка завершена")


if __name__ == '__main__':
    asyncio.run(main())
