from fastapi import APIRouter
from starlette.templating import Jinja2Templates

from fastapi import Request

router = APIRouter()

templates = Jinja2Templates(directory="static")


@router.get("/unauthorized")
def show_unauthorized_page(request: Request):
    return templates.TemplateResponse("Unauthorized.html", {"request": request})
