from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth.models import User
from api.config.models import Config


async def get_config_names(session: AsyncSession, user: User):
    query = select(Config.name).where(Config.user == user.id)
    res = (await session.execute(query)).all()
    return res


async def get_config_info(session: AsyncSession, config_name: str, config_user: int):
    query = select(Config).where(and_(Config.name == config_name, Config.user == config_user))
    result = (await session.execute(query)).scalars().first()
    return result