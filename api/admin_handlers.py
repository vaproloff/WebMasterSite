from fastapi import APIRouter
from fastapi import Request
from fastapi import Form
from fastapi.encoders import jsonable_encoder
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse
from datetime import datetime
from itertools import groupby

from db.session import async_session
from api.actions.urls import _get_urls_with_pagination

admin_router = APIRouter()

templates = Jinja2Templates(directory="static")


def pad_list_with_zeros(lst, amount):
    if len(lst) < amount:
        padding = [0] * (amount - len(lst))
        lst.extend(padding)
    return lst


@admin_router.post("/get")
async def get_urls(request: Request, length: int = Form(), start: int = Form(), start_date: datetime = Form(default=""),
                   end_date: datetime = Form(default=""), amount: int = Form()):
    print(start_date)
    print(end_date)
    limit = length
    offset = start + 1
    urls = await _get_urls_with_pagination(offset, limit, async_session)
    try:
        grouped_data = [(key, sorted(list(group)[:14], key=lambda x: x[0])) for key, group in
                        groupby(urls, key=lambda x: x[-1])]
    except TypeError as e:
        print(urls)
        return
    if len(grouped_data) == 0:
        return {"data": []}
    data = {"data": [{
        "url": el[0],
        "statistic": [{
            "date": stat[0],
            "position": stat[1],
            "clicks": stat[2],
            "impression": stat[3],
            "ctr": stat[4],
        } for stat in el[1]]
    }
        for el in grouped_data]}
    data = [[el[0], *[stat[1] for stat in el[1]]] for el in grouped_data]
    data = []
    for el in grouped_data:
        res = [el[0]]
        for stat in el[1]:
            res.append(stat[1])
        res = pad_list_with_zeros(res, amount + 1)
        data.append(res)
    json_data = jsonable_encoder(data)

    return JSONResponse({"data": json_data, "recordsTotal": limit, "recordsFiltered": 50000})


@admin_router.get("/info-urls")
async def get_urls(request: Request):
    response = templates.TemplateResponse("urls-info.html", {"request": request})
    return response
