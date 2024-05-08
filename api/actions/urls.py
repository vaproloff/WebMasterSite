from db.dals import UrlDAL


async def _add_new_urls(urls, session):
    async with session() as s:
        order_dal = UrlDAL(s)
        order_id = await order_dal.add_new_urls(
            urls
        )
        return order_id


async def _get_urls_with_pagination(page, per_page, date_start, date_end, session):
    async with session() as s:
        url_dal = UrlDAL(s)
        urls = await url_dal.get_urls_with_pagination(
            page, per_page, date_start, date_end
        )
        return urls
