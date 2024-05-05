from fastapi import APIRouter
from fastapi import Request
from fastapi.encoders import jsonable_encoder
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse
from datetime import datetime
from itertools import groupby

from db.session import async_session
from api.actions.urls import _get_urls_with_pagination

admin_router = APIRouter()

templates = Jinja2Templates(directory="static")


@admin_router.post("/get")
async def get_urls(request: Request):
    limit = int(request.query_params.get('length', 10))  # количество отображаемых элементов
    offset = int(request.query_params.get('start', 1))  # номер страницы
    start = datetime.now()
    urls = await _get_urls_with_pagination(offset, limit, async_session)
    grouped_data = [(key, sorted(list(group)[:14], key=lambda x: x[0])) for key, group in
                    groupby(urls, key=lambda x: x[-1])]
    print(datetime.now() - start)
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
    json_data = jsonable_encoder(data)

    # return JSONResponse(content=json_data)
    return JSONResponse({"data": json_data, "recordsTotal": 10, "recordsFiltered": 50000})


@admin_router.get("/info-urls")
async def get_urls(request: Request):
    response = templates.TemplateResponse("urls-info.html", {"request": request})
    return response
