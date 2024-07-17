from typing import Callable

from db.dals import MetricQueryDAL


async def _get_approach_query(session: Callable):
    async with session() as s:
        order_dal = MetricQueryDAL(s)
        queries = await order_dal.get_approach_query(
        )
        return queries