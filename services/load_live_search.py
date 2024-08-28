from datetime import datetime
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.config.models import LiveSearchListQuery, QueryLiveSearchYandex
from services.live_search_parser_async import run_script_async

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
        # query_info = await run_script_async(main_domain, lr, approach_query_names)
            query_info = {'кран шаровый': ['https://dn.ru/sharovyi-kran', 33], 'шаровой кран': ['https://dn.ru/sharovyi-kran', 39], 'кран полнопроходной': ['https://dn.ru/sharovyi-kran/teplosnabzhenie/flantcevyi-kran/polnoprohodnoi', 23], 'кран шаровой для воды': ['https://dn.ru/sharovyi-kran', 30], 'кран шаровой полнопроходной': ['https://dn.ru/sharovyi-kran/teplosnabzhenie/rezbovye-kran/polnoprohodnoi/rashwork', 11], 'кран шаровой прайс': ['https://dn.ru/sharovyi-kran', 31], 'кран шаровой стальной': ['https://dn.ru/sharovyi-kran/stalnyye', 17], 'кран шаровой стальной фланцевый': ['https://dn.ru/sharovyi-kran/teplosnabzhenie/flantcevyi-kran', 7], 'кран шаровой фланцевый': ['https://dn.ru/sharovyi-kran/teplosnabzhenie/flantcevyi-kran', 13], 'кран шаровой фланцевый ду50': ['https://dn.ru/sharovyi-kran/teplosnabzhenie/flantcevyi-kran/dn50', 7], 'кран шаровой фланцевый прайс': ['https://dn.ru/sharovyi-kran/teplosnabzhenie/flantcevyi-kran', 9], 'кран шаровой фланцевый цена': ['https://dn.ru/sharovyi-kran/teplosnabzhenie/flantcevyi-kran/dn100', 7], 'кран шаровой цена': ['https://dn.ru/sharovyi-kran/stalnyye', 30], 'кран шаровый купить': ['https://dn.ru/sharovyi-kran', 25], 'краны муфтовые шаровые полнопроходные': ['https://dn.ru/sharovyi-kran/teplosnabzhenie/rezbovye-kran/polnoprohodnoi/rashwork', 7], 'краны стальные шаровые краны стальные шаровые': ['https://dn.ru/sharovyi-kran/stalnyye', 26], 'краны шаровые для воды': ['https://dn.ru/sharovyi-kran', 32], 'краны шаровые прайс': ['https://dn.ru/sharovyi-kran', 22], 'краны шаровые стальные фланцевые': ['https://dn.ru/sharovyi-kran/teplosnabzhenie/flantcevyi-kran', 10], 'полнопроходные шаровые краны': ['https://dn.ru/sharovyi-kran/teplosnabzhenie/flantcevyi-kran/polnoprohodnoi/rashwork', 14], 'стальной шаровой кран': ['https://dn.ru/sharovyi-kran/stalnyye', 16], 'цена кранов шаровых': ['https://dn.ru/sharovyi-kran/teplosnabzhenie/flantcevyi-kran', 17]}
            query_info_for_db = list()

            for key, value in query_info.items():
                query_info_for_db.append(QueryLiveSearchYandex(
                    query=approach_query[key],
                    url=value[0],
                    position=value[1],
                    date=datetime.strptime(datetime.now().strftime(date_format), date_format)
                ))
            
            session.add_all(query_info_for_db)
            await session.commit()

    except Exception as e:
        print(
            f"Произошло досрочное выключение xmlstock. Search System:{search_system} Ошибка: {e}")



