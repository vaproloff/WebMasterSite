from cmath import inf

from fastapi import APIRouter, HTTPException
from fastapi import Request
from fastapi import Form
from fastapi.encoders import jsonable_encoder
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse
from datetime import datetime
from datetime import timedelta
from itertools import groupby

from api.actions.indicators import _get_indicators_from_db
from api.actions.query_url_merge import _get_merge_with_pagination, _get_merge_query, _get_merge_with_pagination_sort, \
    _get_merge_with_pagination_and_like, _get_merge_with_pagination_and_like_sort
from api.actions.utils import get_day_of_week
from db.models import QueryUrlsMergeLogs
from db.session import async_session
from api.actions.urls import _get_urls_with_pagination
from api.actions.urls import _get_urls_with_pagination_and_like
from api.actions.urls import _get_urls_with_pagination_sort
from api.actions.urls import _get_urls_with_pagination_and_like_sort

from api.actions.queries import _get_urls_with_pagination_query
from api.actions.queries import _get_urls_with_pagination_and_like_query
from api.actions.queries import _get_urls_with_pagination_sort_query
from api.actions.queries import _get_urls_with_pagination_and_like_sort_query

from fastapi.responses import FileResponse
from fastapi.responses import StreamingResponse
from openpyxl import Workbook
import io

import csv
import tempfile

from db.utils import get_last_update_date, get_all_dates

admin_router = APIRouter()

templates = Jinja2Templates(directory="static")

date_format_2 = "%Y-%m-%d"
date_format_out = "%d.%m.%Y"


def pad_list_with_zeros_excel(lst, amount):
    if len(lst) < amount:
        padding = [0] * (amount - len(lst))
        lst.extend(padding)
    return lst


@admin_router.get("/menu")
async def show_menu_page(request: Request):
    return templates.TemplateResponse("menu.html", {"request": request})


@admin_router.post("/generate_excel_url/")
async def generate_excel(request: Request, data_request: dict):
    wb = Workbook()
    ws = wb.active
    start_date = datetime.strptime(data_request["start_date"], date_format_2)
    end_date = datetime.strptime(data_request["end_date"], date_format_2)
    main_header = []
    for i in range(int(data_request["amount"])):
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
    main_header = main_header[::-1]
    main_header.insert(0, "Url")
    for i in range(4):
        main_header.insert(1, "Result")
    ws.append(main_header)
    header = ["Position", "Click", "R", "CTR"] * (int(data_request["amount"]) + 4)
    header.insert(0, "")
    ws.append(header)
    start = 0
    main_header = []
    for i in range(int(data_request["amount"])):
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
    main_header.append("Result")
    main_header = main_header[::-1]
    while True:
        start_el = (start * 50) + 1
        if data_request["sort_result"]:
            if data_request["search_text"] == "":
                urls = await _get_urls_with_pagination_sort(start_el, data_request["length"],
                                                            start_date, end_date,
                                                            data_request["sort_desc"], async_session)
            else:
                urls = await _get_urls_with_pagination_and_like_sort(start_el, data_request["length"],
                                                                     start_date, end_date,
                                                                     data_request["search_text"],
                                                                     data_request["sort_desc"],
                                                                     async_session)
        else:
            if data_request["search_text"] == "":
                urls = await _get_urls_with_pagination(start_el, data_request["length"],
                                                       start_date, end_date, async_session)
            else:
                urls = await _get_urls_with_pagination_and_like(start_el, data_request["length"],
                                                                start_date, end_date,
                                                                data_request["search_text"],
                                                                async_session)
        start += 1
        try:
            if urls:
                urls.sort(key=lambda x: x[-1])
            grouped_data = [(key, sorted(list(group)[:14], key=lambda x: x[0])) for key, group in
                            groupby(urls, key=lambda x: x[-1])]
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
                count += 1
            info["Result"] = [round(position / count, 2), total_clicks, impressions, round(ctr / count, 2)]
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


@admin_router.post("/generate_csv_urls/")
async def generate_excel(request: Request, data_request: dict):
    ws = []
    start_date = datetime.strptime(data_request["start_date"], date_format_2)
    end_date = datetime.strptime(data_request["end_date"], date_format_2)
    main_header = []
    for i in range(int(data_request["amount"])):
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
    main_header = main_header[::-1]
    main_header.insert(0, "Url")
    ws.append(main_header)
    header = ["Position", "Click", "R", "CTR"] * (int(data_request["amount"]) + 4)
    header.insert(0, "")
    for i in range(4):
        main_header.insert(1, "Result")
    ws.append(header)
    start = 0
    main_header = []
    for i in range(int(data_request["amount"])):
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
    main_header.append("Result")
    main_header = main_header[::-1]
    while True:
        start_el = (start * 50) + 1
        if data_request["sort_result"]:
            if data_request["search_text"] == "":
                urls = await _get_urls_with_pagination_sort(start_el, data_request["length"],
                                                            start_date, end_date,
                                                            data_request["sort_desc"], async_session)
            else:
                urls = await _get_urls_with_pagination_and_like_sort(start_el, data_request["length"],
                                                                     start_date, end_date,
                                                                     data_request["search_text"],
                                                                     data_request["sort_desc"],
                                                                     async_session)
        else:
            if data_request["search_text"] == "":
                urls = await _get_urls_with_pagination(start_el, data_request["length"],
                                                       start_date, end_date, async_session)
            else:
                urls = await _get_urls_with_pagination_and_like(start_el, data_request["length"],
                                                                start_date, end_date,
                                                                data_request["search_text"],
                                                                async_session)
        start += 1
        try:
            if urls:
                urls.sort(key=lambda x: x[-1])
            grouped_data = [(key, sorted(list(group)[:14], key=lambda x: x[0])) for key, group in
                            groupby(urls, key=lambda x: x[-1])]
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
                count += 1
            info["Result"] = [round(position / count, 2), total_clicks, impressions, round(ctr / count, 2)]
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


@admin_router.post("/generate_excel_queries/")
async def generate_excel(request: Request, data_request: dict):
    wb = Workbook()
    ws = wb.active
    start_date = datetime.strptime(data_request["start_date"], date_format_2)
    end_date = datetime.strptime(data_request["end_date"], date_format_2)
    main_header = []
    for i in range(int(data_request["amount"])):
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
    main_header = main_header[::-1]
    main_header.insert(0, "Url")
    for i in range(4):
        main_header.insert(1, "Result")
    ws.append(main_header)
    header = ["Position", "Click", "R", "CTR"] * (int(data_request["amount"]) + 4)
    header.insert(0, "")
    ws.append(header)
    start = 0
    main_header = []
    for i in range(int(data_request["amount"])):
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
    main_header.append("Result")
    main_header = main_header[::-1]
    while True:
        start_el = (start * 50) + 1
        if data_request["sort_result"]:
            if data_request["search_text"] == "":
                urls = await _get_urls_with_pagination_sort_query(start_el, data_request["length"], start_date,
                                                                  end_date, data_request["sort_desc"],
                                                                  async_session)
            else:
                urls = await _get_urls_with_pagination_and_like_sort_query(start_el, data_request["length"],
                                                                           start_date, end_date,
                                                                           data_request["search_text"],
                                                                           data_request["sort_desc"],
                                                                           async_session)
        else:
            if data_request["search_text"] == "":
                urls = await _get_urls_with_pagination_query(start_el, data_request["length"], start_date,
                                                             end_date, async_session)
            else:
                urls = await _get_urls_with_pagination_and_like_query(start_el, data_request["length"],
                                                                      start_date, end_date, data_request["search_text"],
                                                                      async_session)
        start += 1
        try:
            if urls:
                urls.sort(key=lambda x: x[-1])
            grouped_data = [(key, sorted(list(group)[:14], key=lambda x: x[0])) for key, group in
                            groupby(urls, key=lambda x: x[-1])]
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
                count += 1
            info["Result"] = [round(position / count, 2), total_clicks, impressions, round(ctr / count, 2)]
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


@admin_router.post("/generate_csv_queries/")
async def generate_excel(request: Request, data_request: dict):
    ws = []
    start_date = datetime.strptime(data_request["start_date"], date_format_2)
    end_date = datetime.strptime(data_request["end_date"], date_format_2)
    main_header = []
    for i in range(int(data_request["amount"])):
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
    main_header = main_header[::-1]
    main_header.insert(0, "Url")
    for i in range(4):
        main_header.insert(1, "Result")
    ws.append(main_header)
    header = ["Position", "Click", "R", "CTR"] * (int(data_request["amount"]) + 4)
    header.insert(0, "")
    ws.append(header)
    start = 0
    main_header = []
    for i in range(int(data_request["amount"])):
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
    main_header.append("Result")
    main_header = main_header[::-1]
    while True:
        start_el = (start * 50) + 1
        if data_request["sort_result"]:
            if data_request["search_text"] == "":
                urls = await _get_urls_with_pagination_sort_query(start_el, data_request["length"], start_date,
                                                                  end_date, data_request["sort_desc"],
                                                                  async_session)
            else:
                urls = await _get_urls_with_pagination_and_like_sort_query(start_el, data_request["length"],
                                                                           start_date, end_date,
                                                                           data_request["search_text"],
                                                                           data_request["sort_desc"],
                                                                           async_session)
        else:
            if data_request["search_text"] == "":
                urls = await _get_urls_with_pagination_query(start_el, data_request["length"], start_date,
                                                             end_date, async_session)
            else:
                urls = await _get_urls_with_pagination_and_like_query(start_el, data_request["length"],
                                                                      start_date, end_date, data_request["search_text"],
                                                                      async_session)
        start += 1
        try:
            if urls:
                urls.sort(key=lambda x: x[-1])
            grouped_data = [(key, sorted(list(group)[:14], key=lambda x: x[0])) for key, group in
                            groupby(urls, key=lambda x: x[-1])]
        except TypeError as e:
            break
        if len(grouped_data) == 0:
            break
        for el in grouped_data:
            res = []
            info = {}
            total_clicks, position, impressions, ctr, count = 0, 0, 0, 0, 0
            for k, stat in enumerate(el[1]):
                info[stat[0].strftime(date_format_out)] = [stat[1], stat[2], stat[3], stat[4]]
                total_clicks += stat[2]
                position += stat[1]
                impressions += stat[3]
                ctr += stat[4]
                count += 1
            info["Result"] = [round(position / count, 2), total_clicks, impressions, round(ctr / count, 2)]
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


def pad_list_with_zeros(lst, amount):
    if len(lst) < amount:
        padding = [f"""<div style='height: 55px; width: 100px; margin: 0px; padding: 0px; background-color: #B9BDBC'>
            <span style='font-size: 18px'><span style='color:red'>NAN</span></span><br>
            <span style='font-size: 10px'>Клики</span><span style='font-size: 10px; margin-left: 20px'>CTR <span style='color:red'>NAN%</span></span><br>
            <span style='font-size: 10px'><span style='color:red'>NAN</span></span> <span style='font-size: 10px; margin-left: 30px'>R <span style='color:red'>NAN%</span></span>
            </div>"""] * (amount - len(lst))
        lst.extend(padding)
    return lst


@admin_router.get("/")
async def login(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})


@admin_router.post("/")
async def login(request: Request, username: str = Form(), password: str = Form()):
    with open('users.txt', 'r') as file:
        for line in file:
            stored_username, stored_password = line.strip().split(':')
            if username == stored_username and password == stored_password:
                return RedirectResponse("/admin/info-urls", status_code=302)
    return HTTPException(status_code=401, detail="Incorrect username or password")


@admin_router.get("/info-urls")
async def get_urls(request: Request):
    response = templates.TemplateResponse("urls-info.html", {"request": request})
    return response


@admin_router.post("/get-urls")
async def get_urls(request: Request, data_request: dict):
    today = datetime.now().date()

    # Вычитаем 14 дней (две недели)
    two_weeks_ago = today - timedelta(days=14)
    start_date = min((datetime.strptime(data_request["start_date"], date_format_2).date()), two_weeks_ago)
    end_date = min((datetime.strptime(data_request["end_date"], date_format_2).date()), datetime.now().date())
    print(end_date)
    if data_request["sort_result"]:
        if data_request["search_text"] == "":
            urls = await _get_urls_with_pagination_sort(data_request["start"], data_request["length"], start_date,
                                                        end_date, data_request["sort_desc"], async_session)
        else:
            urls = await _get_urls_with_pagination_and_like_sort(data_request["start"], data_request["length"],
                                                                 start_date, end_date, data_request["search_text"],
                                                                 data_request["sort_desc"],
                                                                 async_session)
    else:
        if data_request["search_text"] == "":
            urls = await _get_urls_with_pagination(data_request["start"], data_request["length"], start_date, end_date,
                                                   async_session)
        else:
            urls = await _get_urls_with_pagination_and_like(data_request["start"], data_request["length"], start_date,
                                                            end_date, data_request["search_text"],
                                                            async_session)
    try:
        if urls:
            urls.sort(key=lambda x: x[-1])
        grouped_data = [(key, sorted(list(group)[:14], key=lambda x: x[0])) for key, group in
                        groupby(urls, key=lambda x: x[-1])]
    except TypeError as e:
        if urls is None:
            pass
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
            if up > 0:
                color = "#9DE8BD"
                color_text = "green"
            elif up < 0:
                color = "#FDC4BD"
                color_text = "red"
            else:
                color = "#B4D7ED"
                color_text = "black"
            res[stat[0].strftime(
                date_format_2)] = f"""<div style='height: 55px; width: 100px; margin: 0px; padding: 0px; background-color: {color}'>
            <span style='font-size: 18px'>{stat[1]}</span><span style="margin-left: 5px; font-size: 10px; color: {color_text}">{abs(up)}</span><br>
            <span style='font-size: 10px'>Клики</span><span style='font-size: 10px; margin-left: 20px'>CTR {stat[4]}%</span><br>
            <span style='font-size: 10px'>{stat[2]}</span> <span style='font-size: 10px; margin-left: 20px'>R {stat[3]}%</span>
            </div>"""
            total_clicks += stat[2]
            position += stat[1]
            impressions += stat[3]
            ctr += stat[4]
            count += 1
            if k == len(el[1]) - 1:
                res["result"] = res.get("result", "")
                res["result"] += f"""<div style='height: 55px; width: 100px; margin: 0px; padding: 0px; background-color: #9DE8BD'>
                          <span style='font-size: 15px'>Позиция:{round(position / count, 2)}</span>
                          <span style='font-size: 15px'>Клики:{total_clicks}</span>
                          <span style='font-size: 9px'>Показы:{impressions}</span>
                          <span style='font-size: 9px'>ctr:{round(ctr / count, 2)}%</span>
                          </div>"""
        data.append(res)
    json_data = jsonable_encoder(data)

    # return JSONResponse({"data": json_data, "recordsTotal": limit, "recordsFiltered": 50000})
    return JSONResponse({"data": json_data})


@admin_router.get("/info-queries")
async def get_queries(request: Request):
    response = templates.TemplateResponse("queries-info.html", {"request": request})
    return response


@admin_router.post("/get-queries")
async def get_queries(request: Request, data_request: dict):
    today = datetime.now().date()

    # Вычитаем 14 дней (две недели)
    two_weeks_ago = today - timedelta(days=14)
    start_date = datetime.strptime(data_request["start_date"], date_format_2)
    end_date = datetime.strptime(data_request["end_date"], date_format_2)
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
        grouped_data = [(key, sorted(list(group)[:14], key=lambda x: x[0])) for key, group in
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
            if up > 0:
                color = "#9DE8BD"
                color_text = "green"
            elif up < 0:
                color = "#FDC4BD"
                color_text = "red"
            else:
                color = "#B4D7ED"
                color_text = "black"
            res[stat[0].strftime(
                date_format_2)] = f"""<div style='height: 55px; width: 100px; margin: 0px; padding: 0px; background-color: {color}'>
              <span style='font-size: 18px'>{stat[1]}</span><span style="margin-left: 5px; font-size: 10px; color: {color_text}">{abs(up)}</span><br>
              <span style='font-size: 10px'>Клики</span><span style='font-size: 10px; margin-left: 20px'>CTR {stat[4]}%</span><br>
              <span style='font-size: 10px'>{stat[2]}</span> <span style='font-size: 10px; margin-left: 20px'>R {stat[3]}%</span>
              </div>"""
            total_clicks += stat[2]
            position += stat[1]
            impressions += stat[3]
            ctr += stat[4]
            count += 1
            if k == len(el[1]) - 1:
                res["result"] = res.get("result", "")
                res["result"] += f"""<div style='height: 55px; width: 100px; margin: 0px; padding: 0px; background-color: #9DE8BD'>
                          <span style='font-size: 15px'>Позиция:{round(position / count, 2)}</span>
                          <span style='font-size: 15px'>Клики:{total_clicks}</span>
                          <span style='font-size: 9px'>Показы:{impressions}</span>
                          <span style='font-size: 9px'>ctr:{round(ctr / count, 2)}%</span>
                          </div>"""
        data.append(res)

    json_data = jsonable_encoder(data)

    # return JSONResponse({"data": json_data, "recordsTotal": limit, "recordsFiltered": 50000})
    return JSONResponse({"data": json_data})


@admin_router.get("/info-all-history")
async def get_all_history(request: Request):
    response = templates.TemplateResponse("all-history.html", {"request": request})
    return response


@admin_router.post("/get-all-history")
async def post_all_history(request: Request, data_request: dict):
    start_date = datetime.strptime(data_request["start_date"], date_format_2)
    end_date = datetime.strptime(data_request["end_date"], date_format_2)
    indicators = await _get_indicators_from_db(start_date,
                                               end_date, async_session)

    if indicators:
        indicators.sort(key=lambda x: x[0])
    try:
        grouped_data = [(key, sorted(list(group), key=lambda x: x[2])) for key, group in
                        groupby(indicators, key=lambda x: x[0])]
    except TypeError as e:
        return JSONResponse({"data": []})

    if len(grouped_data) == 0:
        return JSONResponse({"data": []})

    data = []
    for count, el in enumerate(grouped_data):
        res = {"query":
                   f"<div style='width:355px; height: 55px; overflow: auto; white-space: nowrap;'><span>{el[0]}</span></div>"}
        prev_value = -inf
        for name, value, date in el[1]:
            if value >= prev_value:
                color = "#9DE8BD"  # green
            else:
                color = "#FDC4BD"  # red
            if value > 0:
                if el[0] != "TOTAL_CTR":
                    res[date.strftime(
                        date_format_2)] = f"""<div style='height: 55px; width: 100px; margin: 0px; padding: 0px; background-color: {color}; text-align: center; display: flex; align-items: center; justify-content: center;'>
                                            <span style='font-size: 18px'>{value}</span>
                                            </div>"""
                else:
                    res[date.strftime(
                        date_format_2)] = f"""<div style='height: 55px; width: 100px; margin: 0px; padding: 0px; background-color: {color}; text-align: center; display: flex; align-items: center; justify-content: center;'>
                                            <span style='font-size: 18px'>{value}%</span>
                                            </div>"""
            prev_value = value
        data.append(res)

    json_data = jsonable_encoder(data)

    # return JSONResponse({"data": json_data, "recordsTotal": limit, "recordsFiltered": 50000})
    return JSONResponse({"data": json_data})


@admin_router.post("/generate_excel_indicators")
async def generate_excel_indicators(request: Request, data_request: dict):
    wb = Workbook()
    ws = wb.active
    start_date = datetime.strptime(data_request["start_date"], date_format_2)
    end_date = datetime.strptime(data_request["end_date"], date_format_2)
    main_header = []
    main_header_day_name = []
    for i in range(int(data_request["amount"])):
        date = (start_date + timedelta(days=i)).strftime(date_format_out)
        main_header_day_name.append(get_day_of_week(date))
        main_header.append(date)
    main_header = main_header[::-1]
    main_header.insert(0, "Indicators")
    main_header_day_name = main_header_day_name[::-1]
    main_header_day_name.insert(0, "День недели")
    ws.append(main_header)
    ws.append(main_header_day_name)
    main_header = []
    for i in range(int(data_request["amount"])):
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
    main_header = main_header[::-1]
    indicators = await _get_indicators_from_db(start_date, end_date, async_session)
    grouped_data = []
    if indicators:
        indicators.sort(key=lambda x: x[0])
    try:
        grouped_data = [(key, sorted(list(group), key=lambda x: x[2])) for key, group in
                        groupby(indicators, key=lambda x: x[0])]
    except TypeError as e:
        print("error")
    if len(grouped_data) == 0:
        print("empty data")
    for el in grouped_data:
        info = {}
        res = []
        for name, value, date in el[1]:
            if name != "TOTAL_CTR":
                info[date.strftime(date_format_out)] = [value]
            else:
                info[date.strftime(date_format_out)] = [f"{value}%"]
        res.append(el[0])
        for el in main_header:
            if el in info:
                res.extend(info[el])
            else:
                res.extend([0])
        ws.append(res)

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)

    return StreamingResponse(io.BytesIO(output.getvalue()),
                             media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={"Content-Disposition": "attachment;filename='data.xlsx'"})


@admin_router.post("/generate_csv_indicators/")
async def generate_excel(request: Request, data_request: dict):
    ws = []
    start_date = datetime.strptime(data_request["start_date"], date_format_2)
    end_date = datetime.strptime(data_request["end_date"], date_format_2)
    main_header = []
    main_header_day_name = []
    for i in range(int(data_request["amount"])):
        date = (start_date + timedelta(days=i)).strftime(date_format_out)
        main_header_day_name.append(get_day_of_week(date))
        main_header.append(date)
    main_header = main_header[::-1]
    main_header.insert(0, "Indicators")
    main_header_day_name = main_header_day_name[::-1]
    main_header_day_name.insert(0, "День недели")
    ws.append(main_header)
    ws.append(main_header_day_name)
    main_header = []
    for i in range(int(data_request["amount"])):
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
    main_header = main_header[::-1]
    indicators = await _get_indicators_from_db(start_date, end_date, async_session)
    grouped_data = []
    if indicators:
        indicators.sort(key=lambda x: x[0])
    try:
        grouped_data = [(key, sorted(list(group), key=lambda x: x[2])) for key, group in
                        groupby(indicators, key=lambda x: x[0])]
    except TypeError as e:
        print("error")
    if len(grouped_data) == 0:
        print("empty data")
    for el in grouped_data:
        info = {}
        res = []
        for name, value, date in el[1]:
            if name != "TOTAL_CTR":
                info[date.strftime(date_format_out)] = [value]
            else:
                info[date.strftime(date_format_out)] = [f"{value}%"]
        res.append(el[0])
        for el in main_header:
            if el in info:
                res.extend(info[el])
            else:
                res.extend([0])
        ws.append(res)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerows(ws)
    output.seek(0)

    return StreamingResponse(content=output.getvalue(),
                             headers={"Content-Disposition": "attachment;filename='data.csv'"})


@admin_router.get("/menu/merge_database/")
async def show_menu_merge_page(request: Request):
    all_dates = await get_all_dates(async_session, QueryUrlsMergeLogs)
    print(all_dates)
    return templates.TemplateResponse("merge_database.html", {"request": request, "all_dates": all_dates})


@admin_router.get("/info-merge")
async def get_info_merge(request: Request):
    date = request.query_params.get("date")
    response = templates.TemplateResponse("query-url-merge.html", {"request": request, "date": date})
    return response


@admin_router.post("/get-merge")
async def post_info_merge(request: Request, data_request: dict):
    today = datetime.now().date()

    # Вычитаем 14 дней (две недели)
    two_weeks_ago = today - timedelta(days=14)
    start_date = min((datetime.strptime(data_request["start_date"], date_format_2).date()), two_weeks_ago)
    end_date = min((datetime.strptime(data_request["end_date"], date_format_2).date()), datetime.now().date())
    if data_request["sort_result"]:
        if data_request["search_text"] == "":
            urls = await _get_merge_with_pagination_sort(data_request["date"], data_request["sort_desc"],
                                                         data_request["start"], data_request["length"],
                                                         async_session)
        else:
            urls = await _get_merge_with_pagination_and_like_sort(data_request["date"], data_request["search_text"],
                                                                  data_request["sort_desc"],
                                                                  data_request["start"], data_request["length"],
                                                                  async_session)
    else:
        if data_request["search_text"] == "":
            urls = await _get_merge_with_pagination(data_request["date"], data_request["start"], data_request["length"],
                                                    async_session)
        else:
            urls = await _get_merge_with_pagination_and_like(data_request["date"], data_request["search_text"],
                                                             data_request["start"], data_request["length"],
                                                             async_session)
    if not urls or len(urls) == 0:
        return JSONResponse({"data": []})
    data = []
    all_queries = list()
    for el in urls:
        all_queries.extend(el[1])
    queries = await _get_merge_query(start_date, end_date, all_queries, async_session)
    if queries:
        queries.sort(key=lambda x: x[-1])
    grouped_data = dict([(key, sorted(list(group)[:14], key=lambda x: x[0])) for key, group in
                         groupby(queries, key=lambda x: x[-1])])
    for el in urls:
        url, queries = el[0], el[1]
        res = {"url":
                   f"<div style='width:355px; height: 55px; overflow: auto; white-space: nowrap;'><span>{el[0]}</span></div>",
               "queries": "", "count": 0}
        for query in queries:
            res["count"] += 1
            res[
                "queries"] += f"<div style='width:355px; height: 55px; overflow: auto; text-align: center; white-space: nowrap;'><span>{query}</span></div>"

            total_clicks, position, impressions, ctr, count = 0, 0, 0, 0, 0
            for k, stat in enumerate(grouped_data.get(query, [None])):
                if stat:
                    up = 0
                    if k + 1 < len(grouped_data[query]):
                        up = round(grouped_data[query][k][1] - grouped_data[query][k - 1][1], 2)
                    if up > 0:
                        color = "#9DE8BD"
                        color_text = "green"
                    elif up < 0:
                        color = "#FDC4BD"
                        color_text = "red"
                    else:
                        color = "#B4D7ED"
                        color_text = "black"
                    res[stat[0].strftime(date_format_2)] = ''.join(res.get(stat[0].strftime(date_format_2), []))
                    res[stat[0].strftime(
                        date_format_2)] += f"""<div style='height: 55px; width: 100px; margin: 0px; padding: 0px; background-color: {color}'>
                                  <span style='font-size: 18px'>{stat[1]}</span><span style="margin-left: 5px; font-size: 10px; color: {color_text}">{abs(up)}</span><br>
                                  <span style='font-size: 10px'>Клики</span><span style='font-size: 10px; margin-left: 20px'>CTR {stat[4]}%</span><br>
                                  <span style='font-size: 10px'>{stat[2]}</span> <span style='font-size: 10px; margin-left: 20px'>R {stat[3]}%</span>
                                  </div>"""
                    total_clicks += stat[2]
                    position += stat[1]
                    impressions += stat[3]
                    ctr += stat[4]
                    count += 1
                    if k == len(grouped_data[query]) - 1:
                        res["result"] = res.get("result", "")
                        res["result"] += f"""<div style='height: 55px; width: 100px; margin: 0px; padding: 0px; background-color: #9DE8BD'>
                                  <span style='font-size: 15px'>Позиция:{round(position / count, 2)}</span>
                                  <span style='font-size: 15px'>Клики:{total_clicks}</span>
                                  <span style='font-size: 9px'>Показы:{impressions}</span>
                                  <span style='font-size: 9px'>ctr:{round(ctr / count, 2)}%</span>
                                  </div>"""
                else:
                    res["result"] = f"""<div style='height: 55px; width: 100px; margin: 0px; padding: 0px; background-color: ##FDC4BD'>
                              <span style='font-size: 20px'>Нет данных</span>
                              </div>"""

        data.append(res)
    json_data = jsonable_encoder(data)
    #
    # # return JSONResponse({"data": json_data, "recordsTotal": limit, "recordsFiltered": 50000})
    return JSONResponse({"data": json_data})


@admin_router.post("/generate_excel_merge/")
async def generate_excel(request: Request, data_request: dict):
    wb = Workbook()
    ws = wb.active
    start_date = datetime.strptime(data_request["start_date"], date_format_2)
    end_date = datetime.strptime(data_request["end_date"], date_format_2)
    main_header = []
    for i in range(int(data_request["amount"])):
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
    main_header = main_header[::-1]
    main_header.insert(0, "Url")
    main_header.insert(1, "Queries")
    for i in range(4):
        main_header.insert(2, "Result")
    ws.append(main_header)
    header = ["Position", "Click", "R", "CTR"] * (int(data_request["amount"]) + 4)
    header.insert(0, "")
    header.insert(0, "")
    ws.append(header)
    start = 0
    main_header = []
    for i in range(int(data_request["amount"])):
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
    main_header.append("Result")
    main_header = main_header[::-1]
    while True:
        start_el = (start * 50) + 1
        if data_request["sort_result"]:
            if data_request["search_text"] == "":
                urls = await _get_merge_with_pagination_sort(data_request["date"], data_request["sort_desc"],
                                                             start_el, data_request["length"],
                                                             async_session)
            else:
                urls = await _get_merge_with_pagination_and_like_sort(data_request["date"], data_request["search_text"],
                                                                      data_request["sort_desc"],
                                                                      start_el, data_request["length"],
                                                                      async_session)
        else:
            if data_request["search_text"] == "":
                urls = await _get_merge_with_pagination(data_request["date"], start_el,
                                                        data_request["length"],
                                                        async_session)
            else:
                urls = await _get_merge_with_pagination_and_like(data_request["date"], data_request["search_text"],
                                                                 start_el, data_request["length"],
                                                                 async_session)
        start += 1
        if not urls or len(urls) == 0:
            break
        all_queries = list()
        for el in urls:
            all_queries.extend(el[1])
        queries = await _get_merge_query(start_date, end_date, all_queries, async_session)
        if queries:
            queries.sort(key=lambda x: x[-1])
        grouped_data = dict([(key, sorted(list(group)[:14], key=lambda x: x[0])) for key, group in
                             groupby(queries, key=lambda x: x[-1])])
        for url, queries in urls:

            for query in queries:
                res = []
                res.append(url)
                print(url, query)
                el = grouped_data.get(query, None)
                info = {}
                if el:
                    total_clicks, position, impressions, ctr, count = 0, 0, 0, 0, 0
                    for k, stat in enumerate(el):
                        info[stat[0].strftime(date_format_out)] = [stat[1], stat[2], stat[3], stat[4]]
                        total_clicks += stat[2]
                        position += stat[1]
                        impressions += stat[3]
                        ctr += stat[4]
                        count += 1
                    info["Result"] = [round(position / count, 2), total_clicks, impressions, round(ctr / count, 2)]
                res.append(query)
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


@admin_router.post("/generate_csv_merge/")
async def generate_excel(request: Request, data_request: dict):
    ws = []
    start_date = datetime.strptime(data_request["start_date"], date_format_2)
    end_date = datetime.strptime(data_request["end_date"], date_format_2)
    main_header = []
    for i in range(int(data_request["amount"])):
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
    main_header = main_header[::-1]
    main_header.insert(0, "Url")
    main_header.insert(1, "Queries")
    for i in range(4):
        main_header.insert(2, "Result")
    ws.append(main_header)
    header = ["Position", "Click", "R", "CTR"] * (int(data_request["amount"]) + 4)
    header.insert(0, "")
    header.insert(0, "")
    ws.append(header)
    start = 0
    main_header = []
    for i in range(int(data_request["amount"])):
        main_header.append((start_date + timedelta(days=i)).strftime(date_format_out))
    main_header.append("Result")
    main_header = main_header[::-1]
    while True:
        start_el = (start * 50) + 1
        if data_request["sort_result"]:
            if data_request["search_text"] == "":
                urls = await _get_merge_with_pagination_sort(data_request["date"], data_request["sort_desc"],
                                                             start_el, data_request["length"],
                                                             async_session)
            else:
                urls = await _get_merge_with_pagination_and_like_sort(data_request["date"], data_request["search_text"],
                                                                      data_request["sort_desc"],
                                                                      start_el, data_request["length"],
                                                                      async_session)
        else:
            if data_request["search_text"] == "":
                urls = await _get_merge_with_pagination(data_request["date"], start_el,
                                                        data_request["length"],
                                                        async_session)
            else:
                urls = await _get_merge_with_pagination_and_like(data_request["date"], data_request["search_text"],
                                                                 start_el, data_request["length"],
                                                                 async_session)
        start += 1
        if not urls or len(urls) == 0:
            break
        all_queries = list()
        for el in urls:
            all_queries.extend(el[1])
        queries = await _get_merge_query(start_date, end_date, all_queries, async_session)
        if queries:
            queries.sort(key=lambda x: x[-1])
        grouped_data = dict([(key, sorted(list(group)[:14], key=lambda x: x[0])) for key, group in
                             groupby(queries, key=lambda x: x[-1])])
        for url, queries in urls:

            for query in queries:
                res = []
                res.append(url)
                print(url, query)
                el = grouped_data.get(query, None)
                info = {}
                if el:
                    total_clicks, position, impressions, ctr, count = 0, 0, 0, 0, 0
                    for k, stat in enumerate(el):
                        info[stat[0].strftime(date_format_out)] = [stat[1], stat[2], stat[3], stat[4]]
                        total_clicks += stat[2]
                        position += stat[1]
                        impressions += stat[3]
                        ctr += stat[4]
                        count += 1
                    info["Result"] = [round(position / count, 2), total_clicks, impressions, round(ctr / count, 2)]
                res.append(query)
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
