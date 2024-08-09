import logging
import re

from alembic import command
from fastapi import APIRouter, Depends, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth.auth_config import current_superuser
from api.auth.models import User
from api.config.models import Config
from api.config.utils import get_config_info
from config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT
from db.session import get_db_general

from alembic.config import Config as AlembicConfig

import asyncpg

router = APIRouter()


@router.post('/add-config')
async def add_config(request: Request,
                     formData: dict,
                     session: AsyncSession = Depends(get_db_general),
                     user: User = Depends(current_superuser)):
    name, database_name, access_token, user_id, host_id = (formData["name"],
                                                           formData["database_name"],
                                                           formData["access_token"],
                                                           formData["user_id"],
                                                           formData["host_id"])

    config = Config(name=name,
                    database_name=database_name,
                    access_token=access_token,
                    user_id=user_id,
                    host_id=host_id,
                    user=user.id)
    session.add(config)
    await session.commit()

    # Подключение к PostgreSQL и создание новой базы данных
    def sanitize_database_name(name):
        # Разрешаем только буквы, цифры и подчеркивания
        if not re.match(r'^[a-zA-Z0-9_]+$', name):
            raise ValueError("Имя базы данных содержит недопустимые символы")
        return name

    try:
        sanitized_database_name = sanitize_database_name(database_name)
        sanitized_database_name_user_bound = f"{sanitized_database_name}_{user.username}"
    except ValueError as e:
        print(e)
        return {"status": 500}

    conn = await asyncpg.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    try:
        print(sanitized_database_name_user_bound)
        await conn.execute(f'CREATE DATABASE {sanitized_database_name_user_bound}')
    except asyncpg.exceptions.DuplicateDatabaseError:
        print("Database already exists")
    finally:
        await conn.close()

    alembic_logger = logging.getLogger('alembic')
    alembic_logger.setLevel(logging.CRITICAL)  # Устанавливаем уровень логирования на CRITICAL

    # Применение миграций Alembic
    alembic_cfg = AlembicConfig("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url",
                                f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{sanitized_database_name_user_bound}")
    command.upgrade(alembic_cfg, "head")

    return {"status": 200}


@router.post("/set-config")
async def set_config(request: Request,
                     config_name: dict,
                     session: AsyncSession = Depends(get_db_general),
                     user: User = Depends(current_superuser)):
    result = await get_config_info(session, config_name['config_name'], user.id)
    request.session["config"] = {"database_name": result.database_name,
                                 "access_token": result.access_token,
                                 "user_id": result.user_id,
                                 "host_id": result.host_id,
                                 "user": user.username
                                 }
    print(request.session)
    return {"status": 200, "details": request.session}
