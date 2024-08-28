from datetime import datetime
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.config.models import LiveSearchListQuery, QueryLiveSearchGoogle, QueryLiveSearchYandex

from services.live_search_parser_async_yandex import run_script_async as run_script_async_yandex
from services.live_search_parser_async_google import run_script_async as run_script_async_google

from const import date_format

async def main(
        list_id:int, 
        main_domain: str, 
        lr:int, 
        search_system: str,
        session: AsyncSession
):
    approach_query = dict((await session.execute(select(LiveSearchListQuery.query, LiveSearchListQuery.id).where(LiveSearchListQuery.list_id == list_id))).fetchall())

    approach_query_names = approach_query.keys()

    try:
        if search_system == "Yandex":
            query_info = await run_script_async_yandex(main_domain, lr, approach_query_names)
            query_info_for_db = list()

            for key, value in query_info.items():
                query_info_for_db.append(QueryLiveSearchYandex(
                    query=approach_query[key],
                    url=value[0],
                    position=value[1],
                    date=datetime.strptime(datetime.now().strftime(date_format), date_format)
                ))
        
        elif search_system == "Google":
            print(approach_query_names)
            query_info = await run_script_async_google(main_domain, lr, approach_query_names)
            query_info_for_db = list()

            print(query_info)

            for key, value in query_info.items():
                query_info_for_db.append(QueryLiveSearchGoogle(
                    query=approach_query[key],
                    url=value[0],
                    position=value[1],
                    date=datetime.strptime(datetime.now().strftime(date_format), date_format)
                ))
        
        print(query_info_for_db)
        session.add_all(query_info_for_db)
        await session.commit()

    except Exception as e:
        print(
            f"Произошло досрочное выключение xmlstock. Search System:{search_system} Ошибка: {e}")



