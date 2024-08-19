from datetime import datetime, timedelta
from itertools import groupby
import logging
import sys
from fastapi import APIRouter, Depends, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import delete, select
from api.actions.queries import _get_urls_with_pagination_and_like_query, _get_urls_with_pagination_and_like_sort_query, _get_urls_with_pagination_query, _get_urls_with_pagination_sort_query
from api.auth.models import User
from api.config.utils import get_config_names, get_group_names
from db.models import LastUpdateDate, MetricsQuery
from db.session import connect_db, get_db_general

from sqlalchemy.ext.asyncio import AsyncSession

from api.auth.auth_config import current_user

from db.utils import get_last_update_date

from const import date_format_2

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler(sys.stdout)
log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
stream_handler.setFormatter(log_formatter)
logger.addHandler(stream_handler)

templates = Jinja2Templates(directory="static")


router = APIRouter()

@router.get("/")
async def get_queries(request: Request, 
                      user: User = Depends(current_user),
                      session: AsyncSession = Depends(get_db_general)
                      ):
    group_name = request.session["group"].get("name", "")

    DATABASE_NAME = request.session['config'].get('database_name', "")
    
    config_names = [elem[0] for elem in (await get_config_names(session, user, group_name))]

    if DATABASE_NAME:
        async_session = await connect_db(DATABASE_NAME)

    last_load_time = None
    
    async with async_session() as s:
        # Создаем запрос для получения записи с максимальным ID
        stmt = (
            select(LastUpdateDate)
            .order_by(LastUpdateDate.id.desc())
            .limit(1)
        )
            
        result = await s.execute(stmt)
        record = result.scalars().first()

        if record:
            last_load_time = record.date.strftime(date_format_2)


    group_names = await get_group_names(session, user)

    return templates.TemplateResponse("queries-info.html",
                                      {"request": request,
                                       "user": user,
                                       "config_names": config_names,
                                       "group_names": group_names,
                                       "last_update_date": last_load_time,
                                        }
                                       )

@router.post("/")
async def get_queries(request: Request, data_request: dict, user: User = Depends(current_user)):
    DATABASE_NAME = request.session['config'].get('database_name', "")
    group = request.session['group'].get('name', '')
    async_session = await connect_db(DATABASE_NAME)
    start_date = datetime.strptime(data_request["start_date"], date_format_2)
    end_date = datetime.strptime(data_request["end_date"], date_format_2)
    logger.info(f"connect to database: {DATABASE_NAME}")
    if data_request["sort_result"]:
        if data_request["search_text"] == "":
            urls = await _get_urls_with_pagination_sort_query(data_request["start"], data_request["length"], start_date,
                                                              end_date, data_request["sort_desc"],
                                                              async_session)
        else:
            urls = await _get_urls_with_pagination_and_like_sort_query(data_request["start"], data_request["length"],
                                                                       start_date, end_date,
                                                                       data_request["search_text"],
                                                                       data_request["sort_desc"],
                                                                       async_session)
    else:
        if data_request["search_text"] == "":
            urls = await _get_urls_with_pagination_query(data_request["start"], data_request["length"], start_date,
                                                         end_date, async_session)
        else:
            urls = await _get_urls_with_pagination_and_like_query(data_request["start"], data_request["length"],
                                                                  start_date, end_date, data_request["search_text"],
                                                                  async_session)
    try:
        if urls:
            urls.sort(key=lambda x: x[-1])
        grouped_data = [(key, sorted(list(group), key=lambda x: x[0])) for key, group in
                        groupby(urls, key=lambda x: x[-1])]
    except TypeError as e:
        return JSONResponse({"data": []})

    if len(grouped_data) == 0:
        return JSONResponse({"data": []})

    data = []
    for el in grouped_data:
        res = {"query":
                   f"<div style='width:355px; height: 55px; overflow: auto; white-space: nowrap;'><span>{el[0]}</span></div>"}
        total_clicks, position, impressions, ctr, count = 0, 0, 0, 0, 0
        for k, stat in enumerate(el[1]):
            up = 0
            if k + 1 < len(el[1]):
                up = round(el[1][k][1] - el[1][k - 1][1], 2)
            color = "#9DE8BD"
            color_text = "green"
            if up > 0:
                color = "#FDC4BD"
                color_text = "red"
            if stat[1] <= 3:
                color = "#B4D7ED"
                color_text = "blue"
            res[stat[0].strftime(
                date_format_2)] = f"""<div style='height: 55px; width: 100px; margin: 0px; padding: 0px; background-color: {color}'>
              <span style='font-size: 18px'>{stat[1]}</span><span style="margin-left: 5px; font-size: 10px; color: {color_text}">{abs(up)}</span><br>
              <span style='font-size: 10px'>Клики</span><span style='font-size: 10px; margin-left: 10px'>CTR {stat[4]}%</span><br>
              <span style='font-size: 10px'>{stat[2]}</span> <span style='font-size: 10px; margin-left: 20px'>R {int(stat[3])}</span>
              </div>"""
            total_clicks += stat[2]
            position += stat[1]
            impressions += stat[3]
            ctr += stat[4]
            if stat[1] > 0:
                count += 1
            if k == len(el[1]) - 1:
                res["result"] = res.get("result", "")
                if count > 0:
                    res["result"] += f"""<div style='height: 55px; width: 100px; margin: 0px; padding: 0px; background-color: #9DE8BD'>
                              <span style='font-size: 15px'>Позиция:{round(position / count, 2)}</span>
                              <span style='font-size: 15px'>Клики:{total_clicks}</span>
                              <span style='font-size: 8px'>Показы:{impressions}</span>
                              <span style='font-size: 7px'>ctr:{round(ctr / count, 2)}%</span>
                              </div>"""
                else:
                    res["result"] += f"""<div style='height: 55px; width: 100px; margin: 0px; padding: 0px; background-color: #9DE8BD'>
                              <span style='font-size: 15px'>Позиция:{0}</span>
                              <span style='font-size: 15px'>Клики:{total_clicks}</span>
                              <span style='font-size: 8px'>Показы:{impressions}</span>
                              <span style='font-size: 8px'>ctr:{0}%</span>
                              </div>"""
        data.append(res)

    json_data = jsonable_encoder(data)

    logger.info("get query data success")
    # return JSONResponse({"data": json_data, "recordsTotal": limit, "recordsFiltered": 50000})
    return JSONResponse({"data": json_data})

@router.delete("/")
async def delete_query(
    request: Request,
    days: int,
    user: User = Depends(current_user),
):

    DATABASE_NAME = request.session["config"]["database_name"]

    session = await connect_db(DATABASE_NAME)

    last_update_date = await get_last_update_date(session, MetricsQuery)

    async with session() as async_session:

        target_date = last_update_date - timedelta(days=days)

        query = delete(MetricsQuery).where(MetricsQuery.date >= target_date)

        await async_session.execute(query)

        await async_session.commit()

    logger.info(f"Из таблицы query были удалены данные до: {target_date}")

    return {
        "status:": "success",
        "message": "delete data for None days",
        }