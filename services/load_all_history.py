import asyncio
import json
from datetime import datetime, timedelta

import requests
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

import config
from api.actions.indicators import _add_new_indicators
from db.models import QueryIndicator, UpdateLogsIndicator, UpdateLogsIndicator
from db.session import get_db

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
    response = requests.get(create_url(last_update_date.date()), headers={'Authorization': f'OAuth {ACCESS_TOKEN}',
                                                                   "Content-Type": "application/json; charset=UTF-8"})

    return response


async def add_data(response: requests.models.Response):
    indicators = response.json()["indicators"]

    data_for_db = list()
    for indicator in indicators:
        for element in indicators[indicator]:
            data_for_db.append(QueryIndicator(indicator=indicator,
                                              value=round(element["value"], 1),
                                              date=datetime.strptime(element["date"].split("T")[0], date_format)))

            await _add_new_indicators(data_for_db, async_session)
    return data_for_db


async def main():
    response = await get_response(async_session)
    await add_data(response)
    await add_last_update_date(async_session, UpdateLogsIndicator, days=2)


if __name__ == '__main__':
    asyncio.run(main())
