from fastapi import APIRouter, Depends, HTTPException
from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates

from sqlalchemy import and_, delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth.auth_config import current_user, RoleChecker
from api.auth.models import User
from api.config.models import Config, Group, List, ListURI
from api.config.utils import get_config_names, get_group_names, get_lists_names
from db.session import get_db_general

from api.query_api.router import router as query_router
from api.url_api.router import router as url_router
from api.history_api.router import router as history_router
from api.merge_api.router import router as merge_router

from sqlalchemy.exc import IntegrityError

admin_router = APIRouter()

admin_router.include_router(query_router, prefix="/query")
admin_router.include_router(url_router, prefix="/url")
admin_router.include_router(history_router, prefix="/history")
admin_router.include_router(merge_router, prefix="/merge")

templates = Jinja2Templates(directory="static")


def pad_list_with_zeros_excel(lst, amount):
    if len(lst) < amount:
        padding = [0] * (amount - len(lst))
        lst.extend(padding)
    return lst


def pad_list_with_zeros(lst, amount):
    if len(lst) < amount:
        padding = [f"""<div style='height: 55px; width: 100px; margin: 0px; padding: 0px; background-color: #B9BDBC'>
            <span style='font-size: 18px'><span style='color:red'>NAN</span></span><br>
            <span style='font-size: 10px'>Клики</span><span style='font-size: 10px; margin-left: 20px'>CTR <span style='color:red'>NAN%</span></span><br>
            <span style='font-size: 10px'><span style='color:red'>NAN</span></span> <span style='font-size: 10px; margin-left: 30px'>R <span style='color:red'>NAN%</span></span>
            </div>"""] * (amount - len(lst))
        lst.extend(padding)
    return lst


@admin_router.get("/")
async def login_page(request: Request, user: User = Depends(current_user)):
    return templates.TemplateResponse("login.html", {"request": request, "user": user})


@admin_router.get("/register")
async def register(request: Request, user: User = Depends(current_user)):
    return templates.TemplateResponse("register.html", {"request": request, "user": user})


@admin_router.get("/profile/{username}")
async def show_profile(request: Request,
                       username: str,
                       user=Depends(current_user),
                       session: AsyncSession = Depends(get_db_general)):
    group_name = request.session["group"].get("name", "")
    config_names = [elem[0] for elem in (await get_config_names(session, user, group_name))]

    group_names = await get_group_names(session, user)

    return templates.TemplateResponse("profile.html",
                                      {"request": request,
                                       "user": user,
                                       "config_names": config_names,
                                       "group_names": group_names})


@admin_router.get("/superuser/{username}")
async def show_superuser(
        request: Request,
        user=Depends(current_user),
        session: AsyncSession = Depends(get_db_general),
        required: bool = Depends(RoleChecker(required_permissions={"Superuser"}))
):
    group_name = request.session["group"].get("name", "")
    config_names = [elem[0] for elem in (await get_config_names(session, user, group_name))]

    group_names = await get_group_names(session, user)

    return templates.TemplateResponse("superuser.html",
                                      {"request": request,
                                       "user": user,
                                       "config_names": config_names,
                                       "group_names": group_names})


@admin_router.get("/list/{username}")
async def show_list(
    request: Request,
    user=Depends(current_user),
    session: AsyncSession = Depends(get_db_general),
    required: bool = Depends(RoleChecker(required_permissions={"User", "Administrator", "Superuser"}))
):
    config_id = request.session["config"]["config_id"]
    group_id = request.session["group"]["group_id"]

    group_name = request.session["group"].get("name", "")
    config_names = [elem[0] for elem in (await get_config_names(session, user, group_name))]
    group_names = await get_group_names(session, user)
    list_names = await get_lists_names(session, user, request.session["group"].get("name", ""), config_id, group_id)

    groups = (await session.execute(select(Group.id, Group.name))).all()
    group_dict = {group.id: group.name for group in groups}
    configs = (await session.execute(select(Config.id, Config.name))).all()
    config_dict = {config.id: config.name for config in configs}
    users = (await session.execute(select(User.id, User.username))).all()
    user_dict = {user.id: user.username for user in users}

    return templates.TemplateResponse("lists.html",
                                      {"request": request,
                                       "user": user,
                                       "config_names": config_names,
                                       "group_names": group_names,
                                       "list_names": list_names,
                                       "group_dict": group_dict,
                                       "config_dict": config_dict,
                                       "name_dict": user_dict,
                                       })


@admin_router.post("/list")
async def add_list(
    request: Request,
    data: dict,
    user=Depends(current_user),
    session: AsyncSession = Depends(get_db_general),
    required: bool = Depends(RoleChecker(required_permissions={"Administrator", "Superuser"}))
):

    group_name, config_name, list_name, uri_list, is_public = data.values()

    group_id = (await session.execute(select(Group.id).where(Group.name == group_name))).scalars().first()
    config_id = (await session.execute(select(Config.id).where(Config.name == config_name))).scalars().first()

    new_list = List(
        name=list_name,
        author=user.id,
        is_public=is_public,
        group=group_id,
        config=config_id,
    )

    new_uris = [ListURI(uri=uri.strip(), list=new_list) for uri in uri_list]

    try:
        session.add(new_list)
        session.add_all(new_uris)
        await session.commit()
    except IntegrityError:
        await session.rollback()
        return JSONResponse(
            status_code=400,
            content={"error": "An error occurred while adding the list. Possibly due to database constraints."}
        )
    except Exception as e:
        await session.rollback()
        return JSONResponse(
            status_code=500,
            content={"error": f"An unexpected error occurred: {str(e)}"}
        )

    return {
        "status": "success",
        "message": f"List '{list_name}' created successfully",
        "list_id": new_list.id
    }


@admin_router.put("/list")
async def change_list_visibility(
    request: Request,
    data: dict,
    user=Depends(current_user),
    session: AsyncSession = Depends(get_db_general),
    required: bool = Depends(RoleChecker(required_permissions={"Administrator", "Superuser"}))
):
    
    is_public = data["is_public"]
    list_name = data["name"]

    if is_public is None or list_name is None:
        raise HTTPException(status_code=400, detail="Both 'is_public' and 'name' must be provided")

    # Выполнение запроса для получения списка с указанным именем
    result = await session.execute(select(List).where(List.name == list_name))
    list_item = result.scalars().first()

    # Проверяем, существует ли список
    if not list_item:
        raise HTTPException(status_code=404, detail="List not found")

    # Обновление is_public в зависимости от входных данных
    list_item.is_public = is_public
    await session.commit()

    return {
        "status": 200,
        "message": f"Changed 'is_public' for {list_item.name} to {is_public}"
    }


@admin_router.delete("/list")
async def delete_list(
    request: Request,
    data: dict,
    user=Depends(current_user),
    session: AsyncSession = Depends(get_db_general),
    required: bool = Depends(RoleChecker(required_permissions={"Administrator", "Superuser"}))
):
    list_name = data["name"]

    # Получаем объект списка
    result = await session.execute(select(List).where(List.name == list_name))
    list_to_delete = result.scalars().first()

    if list_to_delete:
        # Удаляем все связанные записи в list_uri
        await session.execute(delete(ListURI).where(ListURI.list_id == list_to_delete.id))

        # Удаляем объект списка
        await session.delete(list_to_delete)
        await session.commit()  # Сохраняем изменения

        return {
            "status": 200,
            "message": f"Successfully deleted list '{list_name}'"
        }
    else:
        return {
            "status": 404,
            "message": f"List '{list_name}' not found"
        }


@admin_router.get("/list/{list_id}/edit")
async def show_edit_list(
    request: Request,
    list_id: int,
    user=Depends(current_user),
    session: AsyncSession = Depends(get_db_general),
    required: bool = Depends(RoleChecker(required_permissions={"User", "Administrator", "Superuser"}))
):

    group_name = request.session["group"].get("name", "")
    config_names = [elem[0] for elem in (await get_config_names(session, user, group_name))]
    group_names = await get_group_names(session, user)

    groups = (await session.execute(select(Group.id, Group.name))).all()
    group_dict = {group.id: group.name for group in groups}
    configs = (await session.execute(select(Config.id, Config.name))).all()
    config_dict = {config.id: config.name for config in configs}

    uri_list = (await session.execute(select(ListURI.uri).where(ListURI.list_id == list_id))).scalars().all()

    return templates.TemplateResponse("edit_list.html",
                                      {"request": request,
                                       "user": user,
                                       "config_names": config_names,
                                       "group_names": group_names,
                                       "group_dict": group_dict,
                                       "config_dict": config_dict,
                                       "uri_list":uri_list,
                                       "list_id": list_id,
                                       })


@admin_router.delete("/list/{list_id}/edit")
async def delete_list_record(
    request: Request,
    list_id: int,
    uri: dict,
    user=Depends(current_user),
    session: AsyncSession = Depends(get_db_general),
    required: bool = Depends(RoleChecker(required_permissions={"User", "Administrator", "Superuser"}))
):
    uri_model = (await session.execute(select(ListURI).where(and_(ListURI.uri == uri["uri"], ListURI.list_id == list_id)))).scalars().first()

    await session.delete(uri_model)

    await session.commit()

    return {
        "status": 200,
        "message": f"delete {uri} record from {list_id} list"
    }


@admin_router.put("/list/{list_id}/edit")
async def change_list_record(
    request: Request,
    list_id: int,
    data: dict,
    user=Depends(current_user),
    session: AsyncSession = Depends(get_db_general),
    required: bool = Depends(RoleChecker(required_permissions={"User", "Administrator", "Superuser"}))
):
    print(data)

    old_uri, new_uri = data.values()

    uri_model = (await session.execute(select(ListURI).where(and_(ListURI.uri == old_uri, ListURI.list_id == list_id)))).scalars().first()

    uri_model.uri = new_uri

    await session.commit()
    
    return {
        "status": 200,
        "message": f"change uri from {old_uri} to {new_uri}"
    }


@admin_router.post("/list/{list_id}/edit")
async def add_uri(
    request: Request,
    list_id: int,   
    data: dict,
    user=Depends(current_user),
    session: AsyncSession = Depends(get_db_general),
    required: bool = Depends(RoleChecker(required_permissions={"User", "Administrator", "Superuser"}))
):
    record = ListURI(
        uri=data["uri"].strip(),
        list_id=list_id
    )

    session.add(record)

    await session.commit()

    return {
        "status": 200,
        "message": f"add {data['uri']} record to {list_id} list"
    }
