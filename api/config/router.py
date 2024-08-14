import logging
import re

from alembic import command
from fastapi import APIRouter, Depends, Request, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.admin_handlers import templates
from api.auth.auth_config import current_superuser, current_user
from api.auth.models import User
from api.config.models import Config, Role, Group
from api.config.utils import get_config_info, get_config_names
from config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT
from db.session import get_db_general

from alembic.config import Config as AlembicConfig

import asyncpg

router = APIRouter()


@router.post('/add-config')
async def add_config(request: Request,
                     formData: dict,
                     session: AsyncSession = Depends(get_db_general),
                     user: User = Depends(current_user)):
    name, database_name, access_token, user_id, host_id = (formData["name"],
                                                           formData["database_name"],
                                                           formData["access_token"],
                                                           formData["user_id"],
                                                           formData["host_id"])

    config = Config(name=name,
                    database_name=database_name,
                    access_token=access_token,
                    user_id=user_id,
                    host_id=host_id)
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
                     user: User = Depends(current_user)):
    result = await get_config_info(session, config_name['config_name'], user.id)
    request.session["config"] = {"database_name": result.database_name,
                                 "access_token": result.access_token,
                                 "user_id": result.user_id,
                                 "host_id": result.host_id,
                                 }
    print(request.session)
    return {"status": 200, "details": request.session}

@router.post("/set-group")
async def set_group(request: Request,
                     group_name: dict,
                     session: AsyncSession = Depends(get_db_general),
                     user: User = Depends(current_user)):
    request.session["group"] = {"name": group_name["group_name"]}
    print(request.session)
    return {"status": 200, "details": request.session}


@router.get("/role")
async def get_roles(
        request: Request,
        user=Depends(current_user),
        session: AsyncSession = Depends(get_db_general),
) -> dict:
    query = select(Role.name)
    result = (await session.execute(query)).scalars().all()  # Получаем все значения в виде списка строк
    return {"roles": result}  # Возвращаем JSON объект с ключом "roles"

@router.get("/username")
async def get_usernames(
        request: Request,
        user=Depends(current_user),
        session: AsyncSession = Depends(get_db_general),
) -> dict:
    query = select(User.username)
    result = (await session.execute(query)).scalars().all()  # Получаем все значения в виде списка строк
    return {"usernames": result}  # Возвращаем JSON объект с ключом "roles"

@router.get("/config")
async def get_configs(
        request: Request,
        user=Depends(current_user),
        session: AsyncSession = Depends(get_db_general),
) -> dict:
    query = select(Config.name)
    result = (await session.execute(query)).scalars().all()  # Получаем все значения в виде списка строк
    return {"configs": result}  # Возвращаем JSON объект с ключом "roles"


@router.post("/add_group")
async def add_group(
        request: Request,
        formData: dict,
        user=Depends(current_user),
        session: AsyncSession = Depends(get_db_general),
):
    try:
        group_name = formData['group_name']
        usernames = formData['usernames']
        configs = formData['configs']

        res = (await session.execute(select(Group).where(Group.name == group_name))).scalar()

        if res:
            return {
                "status": "error",
                "message": "group already exists"
            }

        # Создание новой группы
        new_group = Group(name=group_name)

        # Добавление пользователей
        users = []
        for username in usernames:
            result = await session.execute(select(User).filter_by(username=username))
            user = result.scalars().first()
            if user:
                users.append(user)
            else:
                raise HTTPException(status_code=404, detail=f"User '{username}' not found")

        new_group.users = users

        # Добавление конфигураций
        configs_objects = []
        for config_name in configs:
            result = await session.execute(select(Config).filter_by(name=config_name))
            config = result.scalars().first()
            if config:
                configs_objects.append(config)
            else:
                raise HTTPException(status_code=404, detail=f"Config '{config_name}' not found")

        new_group.configs = configs_objects

        # Добавление новой группы в базу данных
        session.add(new_group)
        await session.commit()

        return {"status": "success", "group": new_group.id}

    except Exception as e:
        await session.rollback()
        raise HTTPException(status_code=500, detail=str(e))
