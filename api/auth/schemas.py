import uuid
from typing import Optional

from fastapi_users import schemas


class UserRead(schemas.BaseUser[uuid.UUID]):
    id: int
    username: str
    email: Optional[str] = None


class UserCreate(schemas.BaseUserCreate):
    id: int
    username: str
    email: Optional[str] = None


class UserUpdate(schemas.BaseUserUpdate):
    id: int
    username: str
    email: Optional[str] = None
