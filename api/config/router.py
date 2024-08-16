import logging
import re

from alembic import command
from fastapi import APIRouter, Depends, Request, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth.auth_config import current_user
from api.auth.models import User
from api.config.models import Config, Role, Group
from api.config.utils import get_config_info
from config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT
from db.session import get_db_general

from alembic.config import Config as AlembicConfig

import asyncpg

router = APIRouter()

ROLES_PERMISSIONS = {
    "User": {},
    "Administrator": {"User"},
    "Superuser": {"User", "Administrator", "Superuser"},
}


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

    conn = await asyncpg.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    await conn.execute(f'CREATE DATABASE {database_name}')
    print(f"CREATE DATABASE {database_name}: successfully")
    conn.close()

    # Применение миграций Alembic
    alembic_cfg = AlembicConfig("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url",
                                f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{database_name}")
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
    return {"status": 200, "details": request.session}


@router.post("/set-group")
async def set_group(request: Request,
                    group_name: dict,
                    session: AsyncSession = Depends(get_db_general),
                    user: User = Depends(current_user)):
    request.session["group"] = {"name": group_name["group_name"]}
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
    # Получаем роль текущего пользователя
    user_role = ((await session.execute(select(Role.name).where(Role.id == user.role))).fetchone())[0]

    # Получаем список ролей, которые текущий пользователь может видеть
    allowed_roles = ROLES_PERMISSIONS.get(user_role, set())

    # Получаем пользователей, у которых есть одна из разрешенных ролей
    query = select(User.username).join(Role).where(Role.name.in_(allowed_roles))
    result = (await session.execute(query)).scalars().all()  # Получаем все значения в виде списка строк

    return {"usernames": result}


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


@router.post("/delete_group")
async def delete_group(
        request: Request,
        formData: dict,
        user=Depends(current_user),
        session: AsyncSession = Depends(get_db_general),
):
    try:
        # Извлечение названия группы из данных формы
        name = formData["group_name"]

        # Поиск группы по имени
        query = select(Group).where(Group.name == name)
        result = await session.execute(query)
        group = result.scalars().first()

        if group:
            # Удаление группы
            await session.delete(group)
            await session.commit()
        else:
            return {
                "status": "error",
                "message": "Group does not exist"
            }

        # Удаление баз данных
        conn = await asyncpg.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        try:
            # Получение списка всех баз данных
            databases = await conn.fetch("SELECT datname FROM pg_database WHERE datistemplate = false;")
            for db in databases:
                db_name = db['datname']
                # Удаление баз данных, содержащих в названии '_database_name'
                if f"_{name}" in db_name:
                    await conn.execute(f"DROP DATABASE {db_name}")
        except Exception as e:
            print(f"Error while deleting databases: {e}")
        finally:
            await conn.close()

        return {
            "status": "success",
            "message": f"Group {name} deleted and corresponding databases removed"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add-user-to-group")
async def add_user_to_group(
        request: Request,
        formData: dict,
        user=Depends(current_user),
        session: AsyncSession = Depends(get_db_general),
):
    group_name, username = formData.values()

    user_role = ((await session.execute(select(Role.name).where(Role.id == user.role))).fetchone())[0]
    group = ((await session.execute(select(Group).where(Group.name == group_name))).fetchone())
    new_user, role = (await session.execute(
        select(User, Role.name)
        .join(Role, User.role == Role.id)
        .where(User.username == username)
    )).fetchone()

    if group is None and user is None:
        raise HTTPException(status_code=404, detail="Group and User do not exist.")
    elif role not in ROLES_PERMISSIONS[user_role]:
        raise HTTPException(status_code=403, detail="Вы не можете добавить пользователя с такими правами")
    elif group is None:
        raise HTTPException(status_code=404, detail="Group does not exist.")
    elif user is None:
        raise HTTPException(status_code=404, detail="User does not exist.")

    group = group[0]
    group.users.append(new_user)
    await session.commit()

    return {
        "status": "success",
        "message": f"{username} added to {group_name}"
    }


@router.post("/delete-user-from-group")
async def delete_user_from_group(
        request: Request,
        formData: dict,
        user=Depends(current_user),
        session: AsyncSession = Depends(get_db_general),
):
    group_name, username = formData.values()

    # Получение группы и пользователя из базы данных
    group = (await session.execute(select(Group).where(Group.name == group_name))).fetchone()
    user = (await session.execute(select(User).where(User.username == username))).fetchone()

    # Проверка на существование группы и пользователя
    if group is None and user is None:
        raise HTTPException(status_code=404, detail="Group and User do not exist.")
    elif group is None:
        raise HTTPException(status_code=404, detail="Group does not exist.")
    elif user is None:
        raise HTTPException(status_code=404, detail="User does not exist.")

    group, user = group[0], user[0]

    # Удаление пользователя из группы
    if user in group.users:
        group.users.remove(user)
        await session.commit()
        return {
            "status": "success",
            "message": f"{username} removed from {group_name}"
        }
    else:
        raise HTTPException(status_code=404, detail="User is not in the group.")


@router.post("/add-config-to-group")
async def add_config_to_group(
        request: Request,
        formData: dict,
        user=Depends(current_user),
        session: AsyncSession = Depends(get_db_general),
):
    group_name, config_name = formData.values()

    group = ((await session.execute(select(Group).where(Group.name == group_name))).fetchone())
    config = (await session.execute(select(Config).where(Config.name == config_name))).fetchone()

    if group is None and config is None:
        raise HTTPException(status_code=404, detail="Group and Config do not exist.")
    elif group is None:
        raise HTTPException(status_code=404, detail="Group does not exist.")
    elif config is None:
        raise HTTPException(status_code=404, detail="Config does not exist.")

    group, config = group[0], config[0]
    group.configs.append(config)
    await session.commit()

    return {
        "status": "success",
        "message": f"{config} added to {group_name}"
    }


@router.post("/delete-config-from-group")
async def delete_config_from_group(
        request: Request,
        formData: dict,
        user=Depends(current_user),
        session: AsyncSession = Depends(get_db_general),
):
    group_name, config_name = formData.values()
    group = ((await session.execute(select(Group).where(Group.name == group_name))).fetchone())
    config = (await session.execute(select(Config).where(Config.name == config_name))).fetchone()

    if group is None and config is None:
        raise HTTPException(status_code=404, detail="Group and Config do not exist.")
    elif group is None:
        raise HTTPException(status_code=404, detail="Group does not exist.")
    elif config is None:
        raise HTTPException(status_code=404, detail="Config does not exist.")

    group, config = group[0], config[0]
    group.configs.remove(config)
    await session.commit()

    return {
        "status": "success",
        "message": f"{config} added to {group_name}"
    }
