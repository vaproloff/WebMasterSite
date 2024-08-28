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
    list_name: str = Query(None),
    author: int = Query(None),
    user: User = Depends(current_user),
    session: AsyncSession = Depends(get_db_general)
):  
    print(list_name, author)
    group_name = request.session["group"].get("name", "")
    
    config_names = [elem[0] for elem in (await get_config_names(session, user, group_name))]

    group_names = await get_group_names(session, user)

    return templates.TemplateResponse("queries-info.html",
                                      {"request": request,
                                       "user": user,
                                       "config_names": config_names,
                                       "group_names": group_names,
                                        }
                                       )