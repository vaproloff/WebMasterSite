from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.config.models import Config


async def get_config_names(session: AsyncSession):
    query = select(Config.name)
    res = (await session.execute(query)).all()
    return res


async def get_config_info(session: AsyncSession, config_name):
    query = select(Config).where(Config.name == config_name)
    result = (await session.execute(query)).scalars().first()
    return result