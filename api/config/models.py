from sqlalchemy import Column, String, ForeignKey, Integer
from sqlalchemy.orm import DeclarativeBase

from api.auth.models import User


class Base(DeclarativeBase):
    pass


class Config(Base):
    __tablename__ = "config"

    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(String, nullable=False)
    database_name = Column(String, nullable=False)
    access_token = Column(String, nullable=False)
    user_id = Column(String, nullable=False)
    host_id = Column(String, nullable=False)
    user = Column(ForeignKey(User.id), nullable=False)
