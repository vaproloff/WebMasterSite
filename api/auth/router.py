from fastapi import APIRouter, Request, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.templating import Jinja2Templates

from api.auth.auth_config import current_user
from api.auth.models import User
from api.config.models import Role
from db.session import get_db_general

router = APIRouter()

templates = Jinja2Templates(directory="static")


@router.get("/unauthorized")
async def show_unauthorized_page(request: Request):
    return templates.TemplateResponse("Unauthorized.html", {"request": request})


@router.get("/")
async def show_main_page(request: Request, user: User = Depends(current_user)):
    return templates.TemplateResponse("main_page.html", {"request": request, "user": user})


@router.post("/change_user_role")
async def change_user_role(
        request: Request,
        formData: dict,
        user=Depends(current_user),
        session: AsyncSession = Depends(get_db_general),
):
    username = formData["username"]
    new_role = formData["new_role"]

    query = select(Role.id).where(Role.name == new_role)
    new_role_id = (await session.execute(query)).scalars().first()

    query = select(User.id).where(User.username == username)
    user_id = (await session.execute(query)).scalars().first()

    stmt = select(User).where(User.id == user_id)
    result = await session.execute(stmt)
    user = result.scalars().first()

    if user:
        # Обновите роль пользователя
        print("sdfsdfs")
        user.role = new_role_id
        await session.commit()
    else:
        return {"status": "error",
                "message": "User not found"}

    return {
        "status": "success",
        "detail": "User role updated successfully",
    }


