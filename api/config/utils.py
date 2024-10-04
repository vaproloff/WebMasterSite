from fastapi import HTTPException
from sqlalchemy import case, exists, or_, select, and_, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from sqlalchemy.orm import aliased, selectinload
from api.auth.models import User, GroupUserAssociation
from api.config.models import Config, Group, GroupConfigAssociation, List, LiveSearchList, Role, UserQueryCount, \
    RoleAccess


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


async def get_groups_names_dict(
    session: AsyncSession,
):
    group_dict = (await session.execute(select(Group.id, Group.name))).fetchall()

    return dict(group_dict)


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


async def get_live_search_lists_names(
    session: AsyncSession,
    user: User,
):
    stmt = select(LiveSearchList).where(
        LiveSearchList.author == user.id
    )

    result = await session.execute(stmt)
    return result.scalars().all()


async def get_all_user(
    session: AsyncSession,
):
    users = (await session.execute(select(User,#.id,
                                #User.email, 
                                #User.username, 
                                #User.role,
                                #User.groups,
                                UserQueryCount.query_count.label("query_count")
                                ).outerjoin(UserQueryCount, UserQueryCount.user_id == User.id))
                                ).all()
    users_with_query_count = [
        (user, query_count) for user, query_count in users
    ]

    users_with_query_count.sort(key=lambda x: x[0].id)  # Сортируем по id пользователя

    return users_with_query_count

    #users.sort(key=lambda x: x.id)
    #return users


async def get_all_groups(
    session: AsyncSession,
):
    users = (await session.execute(select(Group))).scalars().all()

    users.sort(key=lambda x: x.id)

    return users


async def get_all_roles(
    session: AsyncSession,
):
    roles = (await session.execute(select(Role.id, Role.name))).fetchall()

    return dict(roles)


async def get_all_groups_for_user(
    session: AsyncSession,
    user_id: int,
):
    stmt = select(
        Group).join(
        GroupUserAssociation, GroupUserAssociation.group_id == Group.id).where(
        GroupUserAssociation.user_id == user_id)
    
    group_names = (await session.execute(stmt)).scalars().all()

    return group_names


async def get_all_configs(
    session: AsyncSession,
    user: User
):
    role_accesses = (await session.execute(select(RoleAccess).where(RoleAccess.role_id == user.role))).scalars().first()

    if not role_accesses:
        raise HTTPException(status_code=404, detail="User role accesses not found")

    if role_accesses.command_panel_full:
        configs = (await session.execute(select(Config).options(selectinload(Config.author)))).scalars().all()
    elif role_accesses.command_panel_own:
        configs = (await session.execute(
            select(Config).where(Config.author_id == user.id).options(selectinload(Config.author))
        )).scalars().all()
    else:
        configs = []

    return configs
