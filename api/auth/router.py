import fastapi_users.router
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.security import OAuth2PasswordRequestForm
from starlette.templating import Jinja2Templates
import httpx

from api.auth.auth_config import fastapi_users, auth_backend

router = APIRouter()

# router.include_router(fastapi_users.get_auth_router(auth_backend))

templates = Jinja2Templates(directory="static")


@router.get("/unauthorized")
def show_unauthorized_page(request: Request):
    return templates.TemplateResponse("Unauthorized.html", {"request": request})
