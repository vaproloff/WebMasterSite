from typing import Callable

from db.dals import MetricQueryDAL


async def _add_new_metrics(urls, session: Callable):
    async with session() as s:
        order_dal = MetricQueryDAL(s)
        order_id = await order_dal.add_new_metrics(
            urls
        )
        return order_id


async def _get_top_data_query(top: int, session: Callable):
    async with session() as s:
        order_dal = MetricQueryDAL(s)
        result = await order_dal.get_top_data(
            top
        )
        return result

