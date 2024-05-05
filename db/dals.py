from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from db.models import Url
from db.models import Metrics


###########################################################
# BLOCK FOR INTERACTION WITH DATABASE IN BUSINESS CONTEXT #
###########################################################


class UrlDAL:
    """Data Access Layer for operating user info"""

    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session

    async def add_new_urls(
            self,
            add_values
    ):
        self.db_session.add_all(add_values)
        await self.db_session.flush()
        return

    async def get_urls_with_pagination(self, page, per_page):
        sub = select(Url).limit(per_page).offset(page - 1 if page == 1 else (page - 1) * per_page).subquery()
        query = select(Metrics.date, Metrics.position, Metrics.clicks, Metrics.impression, Metrics.ctr, sub).join(sub,
                                                                                                                  Metrics.url == sub.c.url).group_by(
            sub.c.url,
            Metrics.date,
            Metrics.position,
            Metrics.clicks,
            Metrics.impression,
            Metrics.ctr,
        )
        res = await self.db_session.execute(query)
        product_row = res.fetchall()
        if len(product_row) != 0:
            return product_row


class MetricDAL:
    """Data Access Layer for operating user info"""

    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session

    async def add_new_metrics(
            self,
            add_values
    ):
        self.db_session.add_all(add_values)
        await self.db_session.flush()
        return
