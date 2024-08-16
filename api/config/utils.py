from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth.models import User, GroupUserAssociation
from api.config.models import Config, Group, GroupConfigAssociation


async def get_config_names(session: AsyncSession, user: User, group_name):
    query = select(Group.id).where(Group.name == group_name)
    group_id = (await session.execute(query)).fetchone()
    if group_id:
        group_id = group_id[0]
    query = (select(Config.name)
             .join(GroupConfigAssociation, GroupConfigAssociation.config_id == Config.id)
             .where(GroupConfigAssociation.group_id == group_id))
    res = (await session.execute(query)).all()
    return res


async def get_config_info(
        session: AsyncSession,
        config_name: str,
        config_user: int
):
    query = select(Config).where(and_(Config.name == config_name))
    result = (await session.execute(query)).scalars().first()
    return result


async def get_group_names(session: AsyncSession, user: User):
    query = (
        select(Group.name)
        .join(GroupUserAssociation, GroupUserAssociation.group_id == Group.id)
        .where(GroupUserAssociation.user_id == user.id)
    )
    result = (await session.execute(query)).all()
    print(result)
    return [row[0] for row in result]