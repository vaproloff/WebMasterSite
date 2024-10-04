from typing import Any, Optional

from pydantic import BaseModel
from sqlalchemy import Boolean, Column, DateTime, String, Integer, ForeignKey
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

    author_id = Column(Integer, ForeignKey("user.id"), nullable=True)
    author = relationship("User", back_populates="configs")


class Role(Base):
    __tablename__ = "roles"

    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(String, nullable=False)
    accesses = relationship("RoleAccess", back_populates="role", uselist=False, cascade="all, delete-orphan")

    def __init__(self, **kw: Any):
        super().__init__(**kw)
        self.accesses = RoleAccess()


class RoleAccess(Base):
    __tablename__ = "role_access"

    id = Column(Integer, autoincrement=True, primary_key=True)
    role_id = Column(Integer, ForeignKey("roles.id"), nullable=False)
    role = relationship("Role", back_populates="accesses")

    queries_full = Column(Boolean, default=False)
    queries_view = Column(Boolean, default=False)
    queries_filter = Column(Boolean, default=False)
    queries_export = Column(Boolean, default=False)
    queries_update = Column(Boolean, default=False)
    queries_sum = Column(Boolean, default=False)

    url_full = Column(Boolean, default=False)
    url_view = Column(Boolean, default=False)
    url_filter = Column(Boolean, default=False)
    url_export = Column(Boolean, default=False)
    url_update = Column(Boolean, default=False)
    url_sum = Column(Boolean, default=False)

    history_full = Column(Boolean, default=False)
    history_view = Column(Boolean, default=False)
    history_export = Column(Boolean, default=False)

    url_query_merge_full = Column(Boolean, default=False)
    url_query_merge_view = Column(Boolean, default=False)
    url_query_merge_run = Column(Boolean, default=False)

    list_panel_full = Column(Boolean, default=False)
    list_panel_view = Column(Boolean, default=False)
    list_panel_create = Column(Boolean, default=False)
    list_panel_edit = Column(Boolean, default=False)
    list_panel_share = Column(Boolean, default=False)

    command_panel_full = Column(Boolean, default=False)
    command_panel_own = Column(Boolean, default=False)

    live_search_full = Column(Boolean, default=False)
    live_search_use = Column(Boolean, default=False)

    user_full = Column(Boolean, default=False)

    group_full = Column(Boolean, default=False)


class RoleAccessUpdate(BaseModel):
    queries_full: Optional[bool] = False
    queries_view: Optional[bool] = False
    queries_filter: Optional[bool] = False
    queries_export: Optional[bool] = False
    queries_update: Optional[bool] = False
    queries_sum: Optional[bool] = False

    url_full: Optional[bool] = False
    url_view: Optional[bool] = False
    url_filter: Optional[bool] = False
    url_export: Optional[bool] = False
    url_update: Optional[bool] = False
    url_sum: Optional[bool] = False

    history_full: Optional[bool] = False
    history_view: Optional[bool] = False
    history_export: Optional[bool] = False

    url_query_merge_full: Optional[bool] = False
    url_query_merge_view: Optional[bool] = False
    url_query_merge_run: Optional[bool] = False

    list_panel_full: Optional[bool] = False
    list_panel_view: Optional[bool] = False
    list_panel_create: Optional[bool] = False
    list_panel_edit: Optional[bool] = False
    list_panel_share: Optional[bool] = False

    command_panel_full: Optional[bool] = False
    command_panel_own: Optional[bool] = False

    live_search_full: Optional[bool] = False
    live_search_use: Optional[bool] = False

    user_full: Optional[bool] = False
    group_full: Optional[bool] = False

    class Config:
        from_attributes = True


class Group(Base):
    __tablename__ = "group"

    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(String, nullable=False)

    # Используем строку для связи с Config и User
    configs = relationship("Config",
                           secondary='group_config_association',
                           back_populates="groups",
                           lazy="selectin"
                           )
    users = relationship("User",
                         secondary='group_user_association', back_populates="groups",
                         lazy="selectin"
                         )


class List(Base):
    __tablename__ = "list"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    author = Column(Integer, ForeignKey("user.id"), nullable=False)
    group = Column(Integer, ForeignKey("group.id"), nullable=False)
    config = Column(Integer, ForeignKey("config.id"), nullable=False)
    is_public = Column(Boolean, nullable=False, default=False)

    uris = relationship("ListURI", back_populates="list")


class ListURI(Base):
    __tablename__ = "list_uri"

    id = Column(Integer, primary_key=True, autoincrement=True)
    uri = Column(String, nullable=False)
    list_id = Column(Integer, ForeignKey("list.id"), nullable=False)

    # Связь с List
    list = relationship("List", back_populates="uris")


class UserQueryCount(Base):
    __tablename__ = "user_query_count"

    user_id = Column(Integer, ForeignKey("user.id", ondelete='CASCADE'), nullable=False, primary_key=True)
    query_count = Column(Integer, nullable=False, default=3000)
    last_update_date = Column(DateTime, nullable=False)


class LiveSearchList(Base):
    __tablename__ = "live_search_list"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    author = Column(Integer, ForeignKey("user.id"), nullable=False)
    main_domain = Column(String, nullable=False)

    # Связь с ListLrSearchSystem
    lr_search_systems = relationship("ListLrSearchSystem", back_populates="live_search_list", cascade="all, delete-orphan")

    # Связь с LiveSearchListQuery
    queries = relationship("LiveSearchListQuery", back_populates="live_search_list", cascade="all, delete-orphan")


class ListLrSearchSystem(Base):
    __tablename__ = "list_lr_search_system"

    id = Column(Integer, primary_key=True, autoincrement=True)
    list_id = Column(Integer, ForeignKey("live_search_list.id"), nullable=False)
    lr = Column(Integer, nullable=False)
    search_system = Column(String, nullable=False)

    # Back-reference to LiveSearchList
    live_search_list = relationship("LiveSearchList", back_populates="lr_search_systems")

    # Relationships to QueryLiveSearchYandex and QueryLiveSearchGoogle with cascade delete
    yandex_results = relationship("QueryLiveSearchYandex", back_populates="lr_list", cascade="all, delete-orphan")
    google_results = relationship("QueryLiveSearchGoogle", back_populates="lr_list", cascade="all, delete-orphan")


class LiveSearchListQuery(Base):
    __tablename__ = "live_search_list_query"

    id = Column(Integer, primary_key=True, autoincrement=True)
    query = Column(String, nullable=False)
    list_id = Column(Integer, ForeignKey("live_search_list.id"), nullable=False)

    # Обратная связь с LiveSearchList
    live_search_list = relationship("LiveSearchList", back_populates="queries")

    # Связи с QueryLiveSearchYandex и QueryLiveSearchGoogle
    yandex_results = relationship("QueryLiveSearchYandex", back_populates="live_search_list_query", cascade="all, delete-orphan")
    google_results = relationship("QueryLiveSearchGoogle", back_populates="live_search_list_query", cascade="all, delete-orphan")


class QueryLiveSearchYandex(Base):
    __tablename__ = "query_live_search_yandex"

    id = Column(Integer, primary_key=True, autoincrement=True)
    query_id = Column(Integer, ForeignKey("live_search_list_query.id"), nullable=False)
    url = Column(String, nullable=False)
    position = Column(Integer, nullable=False)
    date = Column(DateTime, nullable=False)
    lr_list_id = Column(Integer, ForeignKey("list_lr_search_system.id", ondelete="CASCADE"), nullable=False)

    # Back-reference to LiveSearchListQuery
    live_search_list_query = relationship("LiveSearchListQuery", back_populates="yandex_results")

    # Relationship to ListLrSearchSystem with back-population
    lr_list = relationship("ListLrSearchSystem", back_populates="yandex_results")


class QueryLiveSearchGoogle(Base):
    __tablename__ = "query_live_search_google"

    id = Column(Integer, primary_key=True, autoincrement=True)
    query_id = Column(Integer, ForeignKey("live_search_list_query.id"), nullable=False)
    url = Column(String, nullable=False)
    position = Column(Integer, nullable=False)
    date = Column(DateTime, nullable=False)
    lr_list_id = Column(Integer, ForeignKey("list_lr_search_system.id", ondelete="CASCADE"), nullable=False)

    # Back-reference to LiveSearchListQuery
    live_search_list_query = relationship("LiveSearchListQuery", back_populates="google_results")

    # Relationship to ListLrSearchSystem with back-population
    lr_list = relationship("ListLrSearchSystem", back_populates="google_results")


class YandexLr(Base):
    __tablename__ = "yandex_lr"

    id = Column(Integer, primary_key=True, autoincrement=True)
    Geo = Column(String, nullable=False)
    Geoid = Column(Integer, nullable=False)
