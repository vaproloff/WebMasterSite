from fastapi import APIRouter, Request, Depends
from starlette.templating import Jinja2Templates

from api.auth.auth_config import current_user
from api.auth.models import User

router = APIRouter()

templates = Jinja2Templates(directory="static")


@router.get("/unauthorized")
async def show_unauthorized_page(request: Request):
    return templates.TemplateResponse("Unauthorized.html", {"request": request})


@router.get("/")
async def show_main_page(request: Request, user: User = Depends(current_user)):
    return templates.TemplateResponse("main_page.html", {"request": request, "user": user})
