from sqlalchemy import Column, String, Integer, ForeignKey
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class GroupConfigAssociation(Base):
    __tablename__ = 'group_config_association'

    group_id = Column(Integer, ForeignKey('group.id'), primary_key=True)
    config_id = Column(Integer, ForeignKey('config.id'), primary_key=True)


class Config(Base):
    __tablename__ = "config"

    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(String, nullable=False)
    database_name = Column(String, nullable=False)
    access_token = Column(String, nullable=False)
    user_id = Column(String, nullable=False)
    host_id = Column(String, nullable=False)

    # Используем строку для связи с Group
    groups = relationship("Group", secondary='group_config_association', back_populates="configs")


class Role(Base):
    __tablename__ = "roles"

    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(String, nullable=False)


class Group(Base):
    __tablename__ = "group"

    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(String, nullable=False)

    # Используем строку для связи с Config и User
    configs = relationship("Config", secondary='group_config_association', back_populates="groups")
    users = relationship("User", secondary='group_user_association', back_populates="groups")
