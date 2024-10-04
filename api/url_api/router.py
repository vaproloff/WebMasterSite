import csv
from datetime import datetime, timedelta
import io
from cmath import inf
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
from api.actions.urls import _get_urls_with_pagination, _get_urls_with_pagination_and_like, _get_urls_with_pagination_and_like_sort, _get_urls_with_pagination_sort, _get_metrics_daily_summary, _get_metrics_daily_summary_like, _get_not_void_count_daily_summary, _get_not_void_count_daily_summary_like
from api.auth.models import User

from api.auth.auth_config import current_user, RoleChecker
from api.config.models import List, RoleAccess
from api.config.utils import get_config_names, get_group_names
from db.models import Metrics
from db.session import connect_db, get_db_general

from const import date_format_out, date_format_2, ACCESS

from sqlalchemy.ext.asyncio import AsyncSession


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler(sys.stdout)
log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
stream_handler.setFormatter(log_formatter)
logger.addHandler(stream_handler)

templates = Jinja2Templates(directory="static")


router = APIRouter()


@router.post("/generate_excel_url")
async def generate_excel_url(
    request: Request, 
    data_request: dict, 
    user: User = Depends(current_user),
    general_session: AsyncSession = Depends(get_db_general),
    required: bool = Depends(RoleChecker(required_accesses={ACCESS.URL_FULL, ACCESS.URL_EXPORT}))
):
    DATABASE_NAME = request.session['config'].get('database_name', "")
    group = request.session['group'].get('name', '')
    state_date = None
    if data_request["button_date"]:
        state_date = datetime.strptime(data_request["button_date"], date_format_2)
    async_session = await connect_db(DATABASE_NAME)
    wb = Workbook()
    ws = wb.active
    start_date = datetime.strptime(data_request["start_date"], date_format_2)
    end_date = datetime.strptime(data_request["end_date"], date_format_2)
    main_header = []
    for i in range(int(data_request["amount"]) + 1):
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
    main_header = main_header[::-1]
    main_header.insert(0, "Url")
    for i in range(4):
        main_header.insert(1, "Result")
    ws.append(main_header)
    header = ["Position", "Click", "R", "CTR"] * (int(data_request["amount"]) + 2)
    header.insert(0, "")
    ws.append(header)
    start = 0
    main_header = []
    for i in range(int(data_request["amount"]) + 1):
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
    main_header.append("Result")
    main_header = main_header[::-1]
    while True:
        start_el = (start * 50)
        if data_request["sort_result"]:
            if data_request["search_text"] == "":
                urls = await _get_urls_with_pagination_sort(
                    start_el, 
                    data_request["length"], 
                    start_date,
                    end_date, 
                    data_request["sort_desc"],
                    data_request["list_name"],
                    async_session,
                    general_session,)
            else:
                urls = await _get_urls_with_pagination_and_like_sort(
                    start_el, 
                    data_request["length"],
                    start_date, 
                    end_date,
                    data_request["search_text"],
                    data_request["sort_desc"],
                    data_request["list_name"],
                    async_session,
                    general_session,)
        else:
            if data_request["search_text"] == "":
                urls = await _get_urls_with_pagination(
                    start_el, 
                    data_request["length"], 
                    start_date,
                    end_date, 
                    data_request["button_state"], 
                    state_date,
                    data_request["metric_type"],
                    data_request["state_type"],
                    data_request["list_name"],
                    async_session,
                    general_session,
                    )
            else:
                urls = await _get_urls_with_pagination_and_like(
                    start_el, 
                    data_request["length"],
                    start_date, 
                    end_date, 
                    data_request["search_text"],
                    data_request["button_state"], 
                    state_date,
                    data_request["metric_type"],
                    data_request["state_type"],
                    data_request["list_name"],
                    async_session,
                    general_session,)
        start += 1
        try:
            if urls:
                urls.sort(key=lambda x: x[-1])
            
            grouped_data = [(key, sorted(list(group), key=lambda x: x[0])) for key, group in
                            groupby(urls, key=lambda x: x[-1])]

            if data_request["button_state"]:
                if state_date and data_request["state_type"] == "date":
                    if data_request["metric_type"] == "P":
                        grouped_data.sort(
                            key=lambda x: next(
                                (
                                    sub_item[1] if sub_item[1] != 0 else 
                                    (-float('inf') if data_request["button_state"] == "decrease" else float('inf'))
                                    for sub_item in x[1]
                                    if sub_item[0] == state_date
                                ),
                                -float('inf') if data_request["button_state"] == "decrease" else float('inf')
                            ),
                            reverse=data_request["button_state"] == "decrease"
                        )
                    elif data_request["metric_type"] == "K":
                        grouped_data.sort(key=lambda x: next((sub_item[2] for sub_item in x[1] if sub_item[0] == state_date), float('-inf')), reverse=data_request["button_state"] == "decrease")
                    elif data_request["metric_type"] == "R":
                        grouped_data.sort(key=lambda x: next((sub_item[3] for sub_item in x[1] if sub_item[0] == state_date), float('-inf')), reverse=data_request["button_state"] == "decrease")
                    elif data_request["metric_type"] == "C":
                        grouped_data.sort(
                            key=lambda x: next(
                                (
                                    sub_item[4] if sub_item[4] != 0 else 
                                    (-float('inf') if data_request["button_state"] == "decrease" else float('inf'))
                                    for sub_item in x[1]
                                    if sub_item[0] == state_date
                                ),
                                -float('inf') if data_request["button_state"] == "decrease" else float('inf')
                            ),
                            reverse=data_request["button_state"] == "decrease"
                        )
                else:
                    if data_request["metric_type"] == "P":
                        grouped_data.sort(
                            key=lambda x: (
                                (total := sum(sub_item[1] for sub_item in x[1] if start_date <= sub_item[0] <= end_date and sub_item[1] != 0),
                                count := sum(1 for sub_item in x[1] if start_date <= sub_item[0] <= end_date and sub_item[1] != 0),
                                total / count if count > 0 else (
                                    -float('inf') if data_request["button_state"] == "decrease" else float('inf')
                                ))[2]
                            ),
                            reverse=data_request["button_state"] == "decrease"
                        )
                    elif data_request["metric_type"] == "K":
                        grouped_data.sort( key=lambda x: (sum(sub_item[2] for sub_item in x[1] if start_date <= sub_item[0] <= end_date)), reverse=data_request["button_state"] == "decrease"),
                    elif data_request["metric_type"] == "R":
                        grouped_data.sort( key=lambda x: (sum(sub_item[3] for sub_item in x[1] if start_date <= sub_item[0] <= end_date)), reverse=data_request["button_state"] == "decrease"),
                    elif data_request["metric_type"] == "C":
                        grouped_data.sort(
                            key=lambda x:(
                                (clicks := (sum(sub_item[2] for sub_item in x[1] if start_date <= sub_item[0] <= end_date)),
                                immersions := (sum(sub_item[3] for sub_item in x[1] if start_date <= sub_item[0] <= end_date)),
                                clicks / immersions if immersions > 0 else (
                                    -float('inf') if data_request["button_state"] == "decrease" else float('inf')
                                ))[2]
                            ),
                            reverse=data_request["button_state"] == "decrease"
                        )   
        
        except TypeError as e:
            break

        if len(grouped_data) == 0:
            break
        for el in grouped_data:
            info = {}
            res = []
            total_clicks, position, impressions, ctr, count = 0, 0, 0, 0, 0
            for k, stat in enumerate(el[1]):
                info[stat[0].strftime(date_format_out)] = [stat[1], stat[2], stat[3], stat[4]]
                total_clicks += stat[2]
                position += stat[1]
                impressions += stat[3]
                if stat[1] > 0:
                    count += 1
            if impressions > 0:
                info["Result"] = [round(position / count, 2), total_clicks, impressions, round(total_clicks * 100 / impressions, 2)]
            else:
                info["Result"] = [0, total_clicks, impressions, 0]
            res.append(el[0])
            for el in main_header:
                if el in info:
                    res.extend(info[el])
                else:
                    res.extend([0, 0, 0, 0])
            ws.append(res)

        output = io.BytesIO()
        wb.save(output)
        output.seek(0)

    return StreamingResponse(io.BytesIO(output.getvalue()),
                            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            headers={"Content-Disposition": "attachment;filename='data.xlsx'"})


@router.post("/generate_csv_urls")
async def generate_csv_url(
    request: Request, 
    data_request: dict, 
    user: User = Depends(current_user),
    general_session: AsyncSession = Depends(get_db_general),
    required: bool = Depends(RoleChecker(required_accesses={ACCESS.URL_FULL, ACCESS.URL_EXPORT}))
):
    DATABASE_NAME = request.session['config'].get('database_name', "")
    group = request.session['group'].get('name', '')
    async_session = await connect_db(DATABASE_NAME)
    ws = []
    state_date = None
    if data_request["button_date"]:
        state_date = datetime.strptime(data_request["button_date"], date_format_2)
    start_date = datetime.strptime(data_request["start_date"], date_format_2)
    end_date = datetime.strptime(data_request["end_date"], date_format_2)
    main_header = []
    for i in range(int(data_request["amount"]) + 1):
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
    main_header = main_header[::-1]
    main_header.insert(0, "Url")
    ws.append(main_header)
    header = ["Position", "Click", "R", "CTR"] * (int(data_request["amount"]) + 2)
    header.insert(0, "")
    for i in range(4):
        main_header.insert(1, "Result")
    ws.append(header)
    start = 0
    main_header = []
    for i in range(int(data_request["amount"]) + 1):
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
    main_header.append("Result")
    main_header = main_header[::-1]
    while True:
        start_el = (start * 50)
        if data_request["sort_result"]:
            if data_request["search_text"] == "":
                urls = await _get_urls_with_pagination_sort(
                    start_el, 
                    data_request["length"], 
                    start_date,
                    end_date, 
                    data_request["sort_desc"],
                    data_request["list_name"],
                    async_session,
                    general_session,)
            else:
                urls = await _get_urls_with_pagination_and_like_sort(
                    start_el, 
                    data_request["length"],
                    start_date, 
                    end_date,
                    data_request["search_text"],
                    data_request["sort_desc"],
                    data_request["list_name"],
                    async_session,
                    general_session,)
        else:
            if data_request["search_text"] == "":
                urls = await _get_urls_with_pagination(
                    start_el, 
                    data_request["length"], 
                    start_date,
                    end_date, 
                    data_request["button_state"], 
                    state_date,
                    data_request["metric_type"],
                    data_request["state_type"],
                    data_request["list_name"],
                    async_session,
                    general_session,
                    )
            else:
                urls = await _get_urls_with_pagination_and_like(
                    start_el, 
                    data_request["length"],
                    start_date, 
                    end_date, 
                    data_request["search_text"],
                    data_request["button_state"], 
                    state_date,
                    data_request["metric_type"],
                    data_request["state_type"],
                    data_request["list_name"],
                    async_session,
                    general_session,)
        start += 1
        try:
            if urls:
                urls.sort(key=lambda x: x[-1])
            
            grouped_data = [(key, sorted(list(group), key=lambda x: x[0])) for key, group in
                            groupby(urls, key=lambda x: x[-1])]

            if data_request["button_state"]:
                if state_date and data_request["state_type"] == "date":
                    if data_request["metric_type"] == "P":
                        grouped_data.sort(
                            key=lambda x: next(
                                (
                                    sub_item[1] if sub_item[1] != 0 else 
                                    (-float('inf') if data_request["button_state"] == "decrease" else float('inf'))
                                    for sub_item in x[1]
                                    if sub_item[0] == state_date
                                ),
                                -float('inf') if data_request["button_state"] == "decrease" else float('inf')
                            ),
                            reverse=data_request["button_state"] == "decrease"
                        )
                    elif data_request["metric_type"] == "K":
                        grouped_data.sort(key=lambda x: next((sub_item[2] for sub_item in x[1] if sub_item[0] == state_date), float('-inf')), reverse=data_request["button_state"] == "decrease")
                    elif data_request["metric_type"] == "R":
                        grouped_data.sort(key=lambda x: next((sub_item[3] for sub_item in x[1] if sub_item[0] == state_date), float('-inf')), reverse=data_request["button_state"] == "decrease")
                    elif data_request["metric_type"] == "C":
                        grouped_data.sort(
                            key=lambda x: next(
                                (
                                    sub_item[4] if sub_item[4] != 0 else 
                                    (-float('inf') if data_request["button_state"] == "decrease" else float('inf'))
                                    for sub_item in x[1]
                                    if sub_item[0] == state_date
                                ),
                                -float('inf') if data_request["button_state"] == "decrease" else float('inf')
                            ),
                            reverse=data_request["button_state"] == "decrease"
                        )
                else:
                    if data_request["metric_type"] == "P":
                        grouped_data.sort(
                            key=lambda x: (
                                (total := sum(sub_item[1] for sub_item in x[1] if start_date <= sub_item[0] <= end_date and sub_item[1] != 0),
                                count := sum(1 for sub_item in x[1] if start_date <= sub_item[0] <= end_date and sub_item[1] != 0),
                                total / count if count > 0 else (
                                    -float('inf') if data_request["button_state"] == "decrease" else float('inf')
                                ))[2]
                            ),
                            reverse=data_request["button_state"] == "decrease"
                        )
                    elif data_request["metric_type"] == "K":
                        grouped_data.sort( key=lambda x: (sum(sub_item[2] for sub_item in x[1] if start_date <= sub_item[0] <= end_date)), reverse=data_request["button_state"] == "decrease"),
                    elif data_request["metric_type"] == "R":
                        grouped_data.sort( key=lambda x: (sum(sub_item[3] for sub_item in x[1] if start_date <= sub_item[0] <= end_date)), reverse=data_request["button_state"] == "decrease"),
                    elif data_request["metric_type"] == "C":
                        grouped_data.sort(
                            key=lambda x:(
                                (clicks := (sum(sub_item[2] for sub_item in x[1] if start_date <= sub_item[0] <= end_date)),
                                immersions := (sum(sub_item[3] for sub_item in x[1] if start_date <= sub_item[0] <= end_date)),
                                clicks / immersions if immersions > 0 else (
                                    -float('inf') if data_request["button_state"] == "decrease" else float('inf')
                                ))[2]
                            ),
                            reverse=data_request["button_state"] == "decrease"
                        )   
            
        except TypeError as e:
            break

        if len(grouped_data) == 0:
            break

        for el in grouped_data:
            info = {}
            res = []
            total_clicks, position, impressions, ctr, count = 0, 0, 0, 0, 0
            for k, stat in enumerate(el[1]):
                info[stat[0].strftime(date_format_out)] = [stat[1], stat[2], stat[3], stat[4]]
                total_clicks += stat[2]
                position += stat[1]
                impressions += stat[3]
                ctr += stat[4]
                if stat[1] > 0:
                    count += 1
            if impressions > 0:
                info["Result"] = [round(position / count, 2), total_clicks, impressions, round(total_clicks * 100 / impressions, 2)]
            else:
                info["Result"] = [0, total_clicks, impressions, 0]
            res.append(el[0])
            for el in main_header:
                if el in info:
                    res.extend(info[el])
                else:
                    res.extend([0, 0, 0, 0])
            ws.append(res)

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerows(ws)
        output.seek(0)

    return StreamingResponse(content=output.getvalue(),
                            headers={"Content-Disposition": "attachment;filename='data.csv'"})


@router.get("/")
async def get_urls(
        request: Request,
        list_name: str = Query(None),
        user: User = Depends(current_user),
        session: AsyncSession = Depends(get_db_general),
        required: bool = Depends(RoleChecker(required_accesses={ACCESS.URL_FULL, ACCESS.URL_VIEW}))
):
    group_name = request.session["group"].get("name", "")
    role_accesses = (await session.execute(select(RoleAccess).where(RoleAccess.role_id == user.role))).scalars().first()
    config_names = [elem[0] for elem in (await get_config_names(session, user, group_name))]

    group_names = await get_group_names(session, user)

    DATABASE_NAME = request.session['config'].get('database_name', "")

    last_load_time, last_date = None, None
    if DATABASE_NAME:
        async_session = await connect_db(DATABASE_NAME)
        last_load_time = await get_last_load_date(async_session, "url")
        last_date = await get_last_date(async_session, Metrics)

    return templates.TemplateResponse("urls-info.html",
                                      {"request": request,
                                       "user": user,
                                       "config_names": config_names,
                                       "group_names": group_names,
                                       "last_update_date": last_load_time,
                                       "list_name": list_name,
                                       "last_date": last_date,
                                       "role_accesses": role_accesses,
                                       }
                                       )


@router.post("/")
async def get_urls(
    request: Request, 
    data_request: dict, 
    user: User = Depends(current_user),
    general_session: AsyncSession = Depends(get_db_general),
    required: bool = Depends(RoleChecker(required_accesses={ACCESS.URL_FULL, ACCESS.URL_UPDATE}))
):
    DATABASE_NAME = request.session['config'].get('database_name', "")
    group = request.session['group'].get('name', '')
    async_session = await connect_db(DATABASE_NAME)

    start_date = datetime.strptime(data_request["start_date"], date_format_2)
    end_date = datetime.strptime(data_request["end_date"], date_format_2)
    state_date = None
    if data_request["button_date"]:
        state_date = datetime.strptime(data_request["button_date"], date_format_2)

    logger.info(f"connect to database: {DATABASE_NAME}")
    if data_request["sort_result"]:
        if data_request["search_text"] == "":
            urls = await _get_urls_with_pagination_sort(
                data_request["start"], 
                data_request["length"], 
                start_date,
                end_date, 
                data_request["sort_desc"],
                data_request["list_name"],
                async_session,
                general_session,)
        else:
            urls = await _get_urls_with_pagination_and_like_sort(
                data_request["start"], 
                data_request["length"],
                start_date, 
                end_date,
                data_request["search_text"],
                data_request["sort_desc"],
                data_request["list_name"],
                async_session,
                general_session,)
    else:
        if data_request["search_text"] == "":
            urls = await _get_urls_with_pagination(
                data_request["start"], 
                data_request["length"], 
                start_date,
                end_date, 
                data_request["button_state"], 
                state_date,
                data_request["metric_type"],
                data_request["state_type"],
                data_request["list_name"],
                async_session,
                general_session,
                )
        else:
            urls = await _get_urls_with_pagination_and_like(
                data_request["start"], 
                data_request["length"],
                start_date, 
                end_date, 
                data_request["search_text"],
                data_request["button_state"], 
                state_date,
                data_request["metric_type"],
                data_request["state_type"],
                data_request["list_name"],
                async_session,
                general_session,)
    try:
        if urls:
            urls.sort(key=lambda x: x[-1])
        
        grouped_data = [(key, sorted(list(group), key=lambda x: x[0])) for key, group in
                        groupby(urls, key=lambda x: x[-1])]

        if data_request["button_state"]:
            if state_date and data_request["state_type"] == "date":
                if data_request["metric_type"] == "P":
                    grouped_data.sort(
                        key=lambda x: next(
                            (
                                sub_item[1] if sub_item[1] != 0 else 
                                (-float('inf') if data_request["button_state"] == "decrease" else float('inf'))
                                for sub_item in x[1]
                                if sub_item[0] == state_date
                            ),
                            -float('inf') if data_request["button_state"] == "decrease" else float('inf')
                        ),
                        reverse=data_request["button_state"] == "decrease"
                    )
                elif data_request["metric_type"] == "K":
                    grouped_data.sort(key=lambda x: next((sub_item[2] for sub_item in x[1] if sub_item[0] == state_date), float('-inf')), reverse=data_request["button_state"] == "decrease")
                elif data_request["metric_type"] == "R":
                    grouped_data.sort(key=lambda x: next((sub_item[3] for sub_item in x[1] if sub_item[0] == state_date), float('-inf')), reverse=data_request["button_state"] == "decrease")
                elif data_request["metric_type"] == "C":
                    grouped_data.sort(
                        key=lambda x: next(
                            (
                                sub_item[4] if sub_item[4] != 0 else 
                                (-float('inf') if data_request["button_state"] == "decrease" else float('inf'))
                                for sub_item in x[1]
                                if sub_item[0] == state_date
                            ),
                            -float('inf') if data_request["button_state"] == "decrease" else float('inf')
                        ),
                        reverse=data_request["button_state"] == "decrease"
                    )
            else:
                if data_request["metric_type"] == "P":
                    grouped_data.sort(
                        key=lambda x: (
                            (total := sum(sub_item[1] for sub_item in x[1] if start_date <= sub_item[0] <= end_date and sub_item[1] != 0),
                            count := sum(1 for sub_item in x[1] if start_date <= sub_item[0] <= end_date and sub_item[1] != 0),
                            total / count if count > 0 else (
                                -float('inf') if data_request["button_state"] == "decrease" else float('inf')
                            ))[2]
                        ),
                        reverse=data_request["button_state"] == "decrease"
                    )
                elif data_request["metric_type"] == "K":
                    grouped_data.sort( key=lambda x: (sum(sub_item[2] for sub_item in x[1] if start_date <= sub_item[0] <= end_date)), reverse=data_request["button_state"] == "decrease"),
                elif data_request["metric_type"] == "R":
                    grouped_data.sort( key=lambda x: (sum(sub_item[3] for sub_item in x[1] if start_date <= sub_item[0] <= end_date)), reverse=data_request["button_state"] == "decrease"),
                elif data_request["metric_type"] == "C":
                    grouped_data.sort(
                        key=lambda x:(
                            (clicks := (sum(sub_item[2] for sub_item in x[1] if start_date <= sub_item[0] <= end_date)),
                            immersions := (sum(sub_item[3] for sub_item in x[1] if start_date <= sub_item[0] <= end_date)),
                            clicks / immersions if immersions > 0 else (
                                -float('inf') if data_request["button_state"] == "decrease" else float('inf')
                            ))[2]
                        ),
                        reverse=data_request["button_state"] == "decrease"
                    )   
        
    except TypeError as e:
        return JSONResponse({"data": []})

    if len(grouped_data) == 0:
        return JSONResponse({"data": []})
    data = []
    for el in grouped_data:
        res = {"url":
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
            if stat[1] > 0:
                count += 1
            if k == len(el[1]) - 1:
                res["result"] = res.get("result", "")
                if count > 0 and impressions > 0:
                    res["result"] += f"""<div style='height: 55px; width: 100px; margin: 0px; padding: 0px; background-color: #9DE8BD'>
                              <span style='font-size: 15px'>Позиция:{round(position / count, 2)}</span>
                              <span style='font-size: 15px'>Клики:{total_clicks}</span>
                              <span style='font-size: 8px'>Показы:{impressions}</span>
                              <span style='font-size: 7px'>ctr:{round(total_clicks * 100 / impressions, 2)}%</span>
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
    return JSONResponse({"data": json_data#, "metricks_data": json_metricks_data
                        })


@router.post("/get_total_sum_urls/")
async def get_total_sum_urls(
    request: Request, 
    data_request: dict, 
    user: User = Depends(current_user),
    general_session: AsyncSession = Depends(get_db_general),
    required: bool = Depends(RoleChecker(required_accesses={ACCESS.URL_FULL, ACCESS.URL_SUM}))
):
    DATABASE_NAME = request.session['config'].get('database_name', "")
    group = request.session['group'].get('name', '')
    async_session = await connect_db(DATABASE_NAME)

    start_date = datetime.strptime(data_request["start_date"], date_format_2)
    end_date = datetime.strptime(data_request["end_date"], date_format_2)
    metricks_data = []

    if data_request["search_text"] == "":
        metricks, total_records = await _get_metrics_daily_summary(
                    start_date,
                    end_date,
                    data_request["list_name"],
                    async_session,
                    general_session,
                    )
    else:
        metricks, total_records = await _get_metrics_daily_summary_like(
                    start_date,
                    end_date,
                    data_request["search_text"], 
                    data_request["list_name"],
                    async_session,
                    general_session,
                    )
    if data_request["search_text"] == "":
        not_void_count_metricks = await _get_not_void_count_daily_summary(
                    start_date,
                    end_date,
                    data_request["list_name"],
                    async_session,
                    general_session,
                    )
    else:
        not_void_count_metricks = await _get_not_void_count_daily_summary_like(
                    start_date,
                    end_date,
                    data_request["search_text"], 
                    data_request["list_name"],
                    async_session,
                    general_session,
                    )
    total_clicks_days = 0
    total_impession_days = 0
    total_not_void = 0
    res_clicks = {"url":
                f"<div style='width:355px; height: 55px; overflow: auto; white-space: nowrap;'><span>Суммарные клики</span></div>"}
    res_impressions = {"url":
                f"<div style='width:355px; height: 55px; overflow: auto; white-space: nowrap;'><span>Суммарные показы</span></div>"}
    res_not_void = {"url":
                f"<div style='width:355px; height: 55px; overflow: auto; white-space: nowrap;'><span>Строки с данными</span></div>"}
    prev_clicks_value = -inf
    prev_impression_value = -inf
    for date, clicks_count, impressions_count in sorted(metricks, key=lambda x: x[0]):
        if clicks_count >= prev_clicks_value:
            color = "#9DE8BD"  # green
        else:
            color = "#FDC4BD"  # red
        if clicks_count > 0:
            res_clicks[date.strftime(
                date_format_2)] = f"""<div style='height: 55px; width: 100px; margin: 0px; padding: 0px; background-color: {color}; text-align: center; display: flex; align-items: center; justify-content: center;'>
                                    <span style='font-size: 18px'>{clicks_count}</span>
                                    </div>"""
            total_clicks_days += clicks_count
        else:
            res_clicks[date.strftime(date_format_2)] = "0"
        prev_clicks_value = clicks_count
        
        if impressions_count >= prev_impression_value:
            color = "#9DE8BD"  # green
        else:
            color = "#FDC4BD"  # red
        if impressions_count > 0:
            res_impressions[date.strftime(
                date_format_2)] = f"""<div style='height: 55px; width: 100px; margin: 0px; padding: 0px; background-color: {color}; text-align: center; display: flex; align-items: center; justify-content: center;'>
                                    <span style='font-size: 18px'>{impressions_count}</span>
                                    </div>"""
            total_impession_days += impressions_count
        else:
            res_impressions[date.strftime(date_format_2)] = "0"
        prev_impression_value = impressions_count

    for date, not_void_count in sorted(not_void_count_metricks, key=lambda x: x[0]):
        total_not_void += not_void_count
        res_not_void[date.strftime(
                date_format_2)] = f"""<div style='height: 55px; width: 100px; margin: 0px; padding: 0px; background-color: #9DE8BD; text-align: center; display: flex; align-items: center; justify-content: center;'>
                                    <span style='font-size: 18px'>{not_void_count}</span>
                                    </div>"""
        
    res_clicks["result"] = f"""<div style='height: 55px; width: 100px; margin: 0px; padding: 0px; background-color: #9DE8BD; text-align: center; display: flex; align-items: center; justify-content: center;'>
                                    <span style='font-size: 18px'>{total_clicks_days}</span>
                                    </div>"""
    res_impressions["result"] = f"""<div style='height: 55px; width: 100px; margin: 0px; padding: 0px; background-color: #9DE8BD; text-align: center; display: flex; align-items: center; justify-content: center;'>
                                    <span style='font-size: 18px'>{total_impession_days}</span>
                                    </div>"""
    res_not_void["result"] = f"""<div style='height: 55px; width: 100px; margin: 0px; padding: 0px; background-color: #9DE8BD; text-align: center; display: flex; align-items: center; justify-content: center;'>
                                    <span style='font-size: 18px'>{total_not_void}</span>
                                    </div>"""
    

    metricks_data.append(res_clicks)
    metricks_data.append(res_impressions)
    metricks_data.append(res_not_void)

    json_total_records = jsonable_encoder(*total_records)
    json_metricks_data = jsonable_encoder(metricks_data)

    logger.info("get query data success")
    # return JSONResponse({"data": json_data, "recordsTotal": limit, "recordsFiltered": 50000})
    return JSONResponse({"metricks_data": json_metricks_data, "total_records": json_total_records
                        })

    #logger.info(f"connect to database: {DATABASE_NAME}")


@router.delete("/")
async def delete_url(
    request: Request,
    days: int,
    user: User = Depends(current_user),
    required: bool = Depends(RoleChecker(required_accesses={ACCESS.URL_FULL, ACCESS.URL_VIEW}))
):
    DATABASE_NAME = request.session["config"]["database_name"]

    session = await connect_db(DATABASE_NAME)

    async with session() as async_session:

        target_date = datetime.now() - timedelta(days=days)

        query = delete(Metrics).where(Metrics.date >= target_date)

        await async_session.execute(query)

        await async_session.commit()

    logger.info(f"Из таблицы url были удалены данные до: {target_date}")

    return {
        "status:": "success",
        "message": "delete data for None days",
        }
