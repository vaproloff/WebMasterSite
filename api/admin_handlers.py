from fastapi import APIRouter, Depends
from fastapi import Request
from fastapi.templating import Jinja2Templates

from sqlalchemy.ext.asyncio import AsyncSession

from api.auth.auth_config import current_user, RoleChecker
from api.auth.models import User
from api.config.utils import get_config_names, get_group_names
from db.session import get_db_general

from api.query_api.router import router as query_router
from api.url_api.router import router as url_router
from api.history_api.router import router as history_router
from api.merge_api.router import router as merge_router

admin_router = APIRouter()

admin_router.include_router(query_router, prefix="/query")
admin_router.include_router(url_router, prefix="/url")
admin_router.include_router(history_router, prefix="/history")
admin_router.include_router(merge_router, prefix="/merge")

templates = Jinja2Templates(directory="static")


def pad_list_with_zeros_excel(lst, amount):
    if len(lst) < amount:
        padding = [0] * (amount - len(lst))
        lst.extend(padding)
    return lst


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
async def login_page(request: Request, user: User = Depends(current_user)):
    return templates.TemplateResponse("login.html", {"request": request, "user": user})


@admin_router.get("/register")
async def register(request: Request, user: User = Depends(current_user)):
    return templates.TemplateResponse("register.html", {"request": request, "user": user})


@admin_router.get("/profile/{username}")
async def show_profile(request: Request,
                       username: str,
                       user=Depends(current_user),
                       session: AsyncSession = Depends(get_db_general)):
    group_name = request.session["group"].get("name", "")
    config_names = [elem[0] for elem in (await get_config_names(session, user, group_name))]

    group_names = await get_group_names(session, user)

    return templates.TemplateResponse("profile.html",
                                      {"request": request,
                                       "user": user,
                                       "config_names": config_names,
                                       "group_names": group_names})


@admin_router.get("/superuser/{username}")
async def show_superuser(
        request: Request,
        user=Depends(current_user),
        session: AsyncSession = Depends(get_db_general),
        required: bool = Depends(RoleChecker(required_permissions={"Administrator", "Superuser"}))
):
    group_name = request.session["group"].get("name", "")
    config_names = [elem[0] for elem in (await get_config_names(session, user, group_name))]

    group_names = await get_group_names(session, user)

    return templates.TemplateResponse("superuser.html",
                                      {"request": request,
                                       "user": user,
                                       "config_names": config_names,
                                       "group_names": group_names})
