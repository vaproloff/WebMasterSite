from alembic import command
from fastapi import APIRouter, Depends, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.config.utils import get_config_info
from config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT
from db.models import Config
from db.session import get_db, get_db_general

from alembic.config import Config as AlembicConfig

import asyncpg

router = APIRouter()


@router.post('/add-config')
async def add_config(request: Request, formData: dict, session: AsyncSession = Depends(get_db_general)):
    name, database_name, access_token, user_id, host_id = (formData["name"],
                                                           formData["database_name"],
                                                           formData["access_token"],
                                                           formData["user_id"],
                                                           formData["host_id"])

    config = Config(name=name, database_name=database_name, access_token=access_token, user_id=user_id, host_id=host_id)
    session.add(config)
    await session.commit()

    # Подключение к PostgreSQL и создание новой базы данных
    conn = await asyncpg.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    try:
        await conn.execute(f'CREATE DATABASE {database_name}')
    except asyncpg.exceptions.DuplicateDatabaseError:
        print("Database already exists")
    finally:
        await conn.close()

    print("QQQQQQ")
    # Применение миграций Alembic
    alembic_cfg = AlembicConfig("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url",
                                f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{database_name}")
    command.upgrade(alembic_cfg, "head")

    return {"status": 200}


@router.post("/set-config")
async def set_config(request: Request, config_name: dict, session: AsyncSession = Depends(get_db_general)):
    result = await get_config_info(session, config_name['config_name'])
    request.session["config"] = {"database_name": result.database_name,
                                 "access_token": result.access_token,
                                 "user_id": result.user_id,
                                 "host_id": result.host_id}
    print(request.session)
    return {"status": 200, "details": request.session}
