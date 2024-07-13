from datetime import datetime, timedelta

from sqlalchemy import select

date_format = "%Y-%m-%d"


async def get_last_update_date(session, db_name):
    async with session() as s:
        query = select(db_name.update_date).order_by(db_name.id.desc()).limit(1)
        result = await s.execute(query)

        row = result.first()

        if row:
            return row[0]

    return


async def add_last_update_date(session, db_name, days=4):
    async with session() as s:
        now = datetime.now()
        three_days_ago = now - timedelta(days=days)
        date_str = three_days_ago.date()

        query = db_name(update_date=date_str)
        s.add(query)

        await s.commit()
