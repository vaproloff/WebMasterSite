import uvicorn
import settings
from fastapi import FastAPI, HTTPException
from fastapi import APIRouter
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from pathlib import Path

from api.admin_handlers import admin_router
from api.auth.auth_config import fastapi_users, auth_backend
from api.auth.http_exception import http_exception_handler
from api.auth.schemas import UserRead, UserCreate

from api.auth.router import router as auth_router

app = FastAPI(
    title="Metrics urls",
    redoc_url=None,
    docs_url="/docs",
)

app.mount("/static", StaticFiles(directory=Path(__file__).parent.absolute() / "static"), name="static")

# CORS

origins = [
    "*"
]

app.add_exception_handler(HTTPException, http_exception_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

main_api_router = APIRouter()
main_api_router.include_router(admin_router, prefix="/admin")

app.include_router(
    fastapi_users.get_auth_router(auth_backend),
    prefix="/auth/jwt",
    tags=["auth"],
)

app.include_router(
    fastapi_users.get_register_router(UserRead, UserCreate),
    prefix="/auth",
    tags=["auth"],
)

app.include_router(main_api_router)

app.include_router(auth_router)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=settings.APP_PORT, reload=True)
