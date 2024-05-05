"""File with settings and configs for the project"""
from envparse import Env

env = Env()

REAL_DATABASE_URL = env.str(
    "REAL_DATABASE_URL",
    default="postgresql+asyncpg://postgres:postgres@0.0.0.0:5484/postgres",
)  # connect string for the real database
APP_PORT = env.int("APP_PORT", default=8888)
