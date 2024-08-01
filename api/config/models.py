from sqlalchemy import Column, String
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class Config(Base):
    __tablename__ = "config"

    name = Column(String, primary_key=True)
    database_name = Column(String, nullable=False)
    access_token = Column(String, nullable=False)
    user_id = Column(String, nullable=False)
    host_id = Column(String, nullable=False)
