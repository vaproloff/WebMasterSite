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


# Новый эндпоинт для логина с перенаправлением
@router.post("/custom-login")
async def custom_login(
        request: Request,
        credentials: OAuth2PasswordRequestForm = Depends(),
):
    # Вызов оригинального эндпоинта логина
    async with httpx.AsyncClient() as client:
        response = await client.post(
            "http://127.0.0.1:8000/auth/jwt/login",  # Замените на правильный URL вашего эндпоинта логина
            data={"username": credentials.username, "password": credentials.password},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

    return RedirectResponse(url="/admin/menu")
