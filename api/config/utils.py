from sqlalchemy import case, exists, or_, select, and_, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from sqlalchemy.orm import aliased

from api.auth.models import User, GroupUserAssociation
from api.config.models import Config, Group, GroupConfigAssociation, List


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
    return [row[0] for row in result]


async def get_lists_names(
    session: AsyncSession,
    user: User,
    current_group: str,
    config_id: int,
    group_id: int,
):
    stmt = select(List).where(
        or_(and_(List.author == user.id, List.config == config_id), case((List.group == group_id, List.is_public == True), else_=False))
    )

    result = await session.execute(stmt)
    return result.scalars().all()