from sqlalchemy import and_, desc, select

from api.config.models import LiveSearchListQuery, QueryLiveSearchYandex


async def get_urls_with_pagination(
    page, 
    per_page, 
    date_start, 
    date_end, 
    state, 
    state_date, 
    metric_type, 
    state_type,
    list_id,
    session,
):
    sub = select(LiveSearchListQuery).where(LiveSearchListQuery.list_id == list_id).offset(page).limit(per_page).subquery()
    
    query = (
        select(
            QueryLiveSearchYandex.date,
            QueryLiveSearchYandex.url,
            QueryLiveSearchYandex.position,
            sub.c.query,
        )
        .join(sub, QueryLiveSearchYandex.query == sub.c.id)
        .group_by(
            QueryLiveSearchYandex.date,
            QueryLiveSearchYandex.url,
            QueryLiveSearchYandex.position,
            sub.c.id,
            sub.c.query,
            sub.c.list_id,
        )
        .having(and_(
            QueryLiveSearchYandex.date <= date_end,
            QueryLiveSearchYandex.date >= date_start
        ))
    )

    res = await session.execute(query)
    product_row = res.fetchall()
    
    if product_row:
        return product_row


async def get_urls_with_pagination_and_like(
    page, 
    per_page, 
    date_start, 
    date_end,
    search_text, 
    state, 
    state_date, 
    metric_type, 
    state_type,
    list_id,
    session,
):
    sub = select(LiveSearchListQuery).where(LiveSearchListQuery.list_id == list_id).filter(LiveSearchListQuery.query.like(f"%{search_text.strip()}%")).offset(page).limit(per_page).subquery()
    
    query = (
        select(
            QueryLiveSearchYandex.date,
            QueryLiveSearchYandex.url,
            QueryLiveSearchYandex.position,
            sub.c.query,
        )
        .join(sub, QueryLiveSearchYandex.query == sub.c.id)
        .group_by(
            QueryLiveSearchYandex.date,
            QueryLiveSearchYandex.url,
            QueryLiveSearchYandex.position,
            sub.c.id,
            sub.c.query,
            sub.c.list_id,
        )
        .having(and_(
            QueryLiveSearchYandex.date <= date_end,
            QueryLiveSearchYandex.date >= date_start
        ))
    )

    res = await session.execute(query)
    product_row = res.fetchall()
    
    if product_row:
        return product_row


async def get_urls_with_pagination_sort(
    page, 
    per_page, 
    date_start, 
    date_end, 
    sort_desc, 
    list_id,
    session,
):
    if sort_desc:
        sub = select(LiveSearchListQuery).order_by(desc(LiveSearchListQuery.query)).where(LiveSearchListQuery.list_id == list_id).offset(page).limit(per_page).subquery()
    else:
        sub = select(LiveSearchListQuery).order_by((LiveSearchListQuery.query)).where(LiveSearchListQuery.list_id == list_id).offset(page).limit(per_page).subquery()
    
    query = (
        select(
            QueryLiveSearchYandex.date,
            QueryLiveSearchYandex.url,
            QueryLiveSearchYandex.position,
            sub.c.query,
        )
        .join(sub, QueryLiveSearchYandex.query == sub.c.id)
        .group_by(
            QueryLiveSearchYandex.date,
            QueryLiveSearchYandex.url,
            QueryLiveSearchYandex.position,
            sub.c.id,
            sub.c.query,
            sub.c.list_id,
        )
        .having(and_(
            QueryLiveSearchYandex.date <= date_end,
            QueryLiveSearchYandex.date >= date_start
        ))
    )

    res = await session.execute(query)
    product_row = res.fetchall()
    
    if product_row:
        return product_row


async def get_urls_with_pagination_sort_and_like(
    page, 
    per_page, 
    date_start, 
    date_end,
    search_text, 
    sort_desc, 
    list_id,
    session,
):
    if sort_desc:
        sub = select(LiveSearchListQuery).order_by(desc(LiveSearchListQuery.query)).filter(LiveSearchListQuery.query.like(f"%{search_text.strip()}%")).where(LiveSearchListQuery.list_id == list_id).offset(page).limit(per_page).subquery()
    else:
        sub = select(LiveSearchListQuery).order_by((LiveSearchListQuery.query)).filter(LiveSearchListQuery.query.like(f"%{search_text.strip()}%")).where(LiveSearchListQuery.list_id == list_id).offset(page).limit(per_page).subquery()
    
    query = (
        select(
            QueryLiveSearchYandex.date,
            QueryLiveSearchYandex.url,
            QueryLiveSearchYandex.position,
            sub.c.query,
        )
        .join(sub, QueryLiveSearchYandex.query == sub.c.id)
        .group_by(
            QueryLiveSearchYandex.date,
            QueryLiveSearchYandex.url,
            QueryLiveSearchYandex.position,
            sub.c.id,
            sub.c.query,
            sub.c.list_id,
        )
        .having(and_(
            QueryLiveSearchYandex.date <= date_end,
            QueryLiveSearchYandex.date >= date_start
        ))
    )

    res = await session.execute(query)
    product_row = res.fetchall()
    
    if product_row:
        return product_row