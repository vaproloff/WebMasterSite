from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Config
from db.session import get_db

router = APIRouter()


@router.post('/add-config')
async def add_config(request: Request, formData: dict, session: AsyncSession = Depends(get_db)):
    name, database_name, access_token, user_id, host_id = (formData["name"],
                                                           formData["database_name"],
                                                           formData["access_token"],
                                                           formData["user_id"],
                                                           formData["host_id"])
    config = Config(name=name, database_name=database_name, access_token=access_token, user_id=user_id, host_id=host_id)
    session.add(config)
    await session.commit()
    return {"status": 200}
