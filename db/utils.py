from datetime import datetime, timedelta

from sqlalchemy import select, distinct

date_format = "%Y-%m-%d"


async def get_last_update_date(session, db_name):
    async with session() as s:
        query = select(db_name.update_date).order_by(db_name.id.desc()).limit(1)
        result = await s.execute(query)

        row = result.first()

        if row:
            return row[0]

    return


async def add_last_update_date(session, db_name, date):
    async with session() as s:
        query = db_name(update_date=date)
        s.add(query)

        await s.commit()


async def get_all_dates(session, db_name):
    async with session() as s:
        query = select(distinct(db_name.update_date))
        res = await s.execute(query)
        res = res.fetchall()
        return res
