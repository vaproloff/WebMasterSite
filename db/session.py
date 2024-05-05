from typing import Generator

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker

##############################################
# BLOCK FOR COMMON INTERACTION WITH DATABASE #
##############################################

# REAL_DATABASE_URL = f"postgresql+asyncpg://mainuser:Kirill123456@5.35.81.91:5432/products"
REAL_DATABASE_URL = f"postgresql+asyncpg://postgres:postgres@0.0.0.0:5484/postgres"

# create async engine for interaction with database
engine = create_async_engine(
    REAL_DATABASE_URL,
    future=True,
    execution_options={"isolation_level": "AUTOCOMMIT"},
)

# create session for the interaction with database
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def get_db() -> Generator:
    """Dependency for getting async session"""
    try:
        session: AsyncSession = async_session()
        yield session
    finally:
        await session.close()
