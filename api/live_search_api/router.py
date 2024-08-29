import csv
from datetime import datetime, timedelta
import io
from itertools import groupby
import logging
import sys
from fastapi import APIRouter, Depends, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from openpyxl import Workbook
from sqlalchemy import delete, select
from api.actions.actions import get_last_date, get_last_load_date
from api.actions.queries import _get_urls_with_pagination_and_like_query, _get_urls_with_pagination_and_like_sort_query, _get_urls_with_pagination_query, _get_urls_with_pagination_sort_query
from api.auth.models import User
from api.config.utils import get_config_names, get_group_names
from api.live_search_api.db import get_urls_with_pagination, get_urls_with_pagination_and_like, get_urls_with_pagination_sort, get_urls_with_pagination_sort_and_like
from db.models import LastUpdateDate, MetricsQuery
from db.session import connect_db, get_db_general

from sqlalchemy.ext.asyncio import AsyncSession

from api.auth.auth_config import current_user

from const import date_format_2, date_format_out


templates = Jinja2Templates(directory="static")


router = APIRouter()

@router.get("/")
async def get_live_search(
    request: Request,
    list_id: int = Query(None),
    author: int = Query(None),
    user: User = Depends(current_user),
    session: AsyncSession = Depends(get_db_general)
):  
    print(list_id, author)
    group_name = request.session["group"].get("name", "")
    
    config_names = [elem[0] for elem in (await get_config_names(session, user, group_name))]

    group_names = await get_group_names(session, user)

    return templates.TemplateResponse("live_search-info.html",
                                      {"request": request,
                                       "user": user,
                                       "config_names": config_names,
                                       "group_names": group_names,
                                       "list_id": list_id,
                                       "author": author,
                                        }
                                       )


@router.post("/")
async def get_live_search(
    request: Request,
    data_request: dict,
    user: User = Depends(current_user),
    session: AsyncSession = Depends(get_db_general)
    ):

    start_date = datetime.strptime(data_request["start_date"], date_format_2)
    end_date = datetime.strptime(data_request["end_date"], date_format_2)
    list_id = int(data_request["list_id"])
    author = int(data_request["author"])
    state_date = None
    if data_request["button_date"]:
        state_date = datetime.strptime(data_request["button_date"], date_format_2)

    if data_request["sort_result"]:
        if data_request["search_text"] == "":
            urls = await get_urls_with_pagination_sort(
                data_request["start"], 
                data_request["length"], 
                start_date,
                end_date,
                data_request["sort_desc"],
                list_id,
                session,
                )
        else:
            urls = await get_urls_with_pagination_sort_and_like(
                data_request["start"], 
                data_request["length"],
                start_date, 
                end_date,
                data_request["search_text"],
                data_request["sort_desc"],
                list_id,
                session
                )
    else:
        if data_request["search_text"] == "":
            urls = await get_urls_with_pagination(
                data_request["start"], 
                data_request["length"], 
                start_date,
                end_date, 
                data_request["button_state"], 
                state_date,
                data_request["metric_type"],
                data_request["state_type"],
                list_id,
                session,
                )
        else:
            urls = await get_urls_with_pagination_and_like(
                data_request["start"], 
                data_request["length"],
                start_date, 
                end_date, 
                data_request["search_text"],
                data_request["button_state"], 
                state_date,
                data_request["metric_type"],
                data_request["state_type"],
                list_id,
                session,
                )
    try:
        if urls:
            urls.sort(key=lambda x: x[-1])
        
        grouped_data = [(key, sorted(list(group), key=lambda x: x[0])) for key, group in
                        groupby(urls, key=lambda x: x[-1])]

        if data_request["button_state"]:
            if data_request["metric_type"] == "P":
                grouped_data.sort(
                    key=lambda x: next(
                        (
                            sub_item[2] if sub_item[2] != 0 else 
                            (-float('inf') if data_request["button_state"] == "decrease" else float('inf'))
                            for sub_item in x[1]
                            if sub_item[0] == state_date
                        ),
                        -float('inf') if data_request["button_state"] == "decrease" else float('inf')
                    ),
                    reverse=data_request["button_state"] == "decrease"
                )
    except TypeError as e:
        return JSONResponse({"data": []})

    if len(grouped_data) == 0:
        return JSONResponse({"data": []})
    
    data = []
    for el in grouped_data:
        res = {"query":
                   f"<div style='width:355px; height: 55px; overflow: auto; white-space: nowrap;'><span>{el[0]}</span></div>"}
        url, pos = "", float("inf")
        for k, stat in enumerate(el[1]):
            if stat[2] < pos:
                color = "#9DE8BD"
                color_text = "green"
            elif stat[2] > pos:
                color = "#FDC4BD"
                color_text = "red"
            else:
                color = "#B4D7ED"
                color_text = "blue"
            pos = stat[2]
            res[stat[0].strftime(
                date_format_2)] = f"""<div style='height: 55px; width: 100px; margin: 0px; padding: 0px; background-color: {color}; text-align: center; display: flex; align-items: center; justify-content: center;'>
                                    <a href='{stat[1]}' style='font-size: 18px; text-decoration: none; color: inherit;'>
                                        {stat[2]}
                                    </a>
                                </div>"""
        data.append(res)

    json_data = jsonable_encoder(data)

    return JSONResponse({"data": json_data})