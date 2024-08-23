from sqlalchemy import exists, or_, select, and_, tuple_
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
    current_group: str
):
    # Получаем group_id для current_group
    group_id_result = await session.execute(
        select(Group.id).where(Group.name == current_group)
    )
    group_id = group_id_result.scalar()

    if group_id is None:
        return []

    # Подзапрос для нахождения групп, к которым принадлежит автор
    author_groups_subquery = select(GroupUserAssociation.group_id).filter(
        GroupUserAssociation.user_id == List.author
    ).subquery()

    author_groups = select(GroupUserAssociation.group_id).filter(
        GroupUserAssociation.user_id == List.author
    )


    # Основной запрос
    stmt = select(List.name).filter(
        or_(
            List.author == user.id,
            and_(
                List.is_public == True,
                tuple_(group_id).in_(author_groups_subquery)
                )
            )
        )

    result = await session.execute(stmt)
    return result.scalars().all()