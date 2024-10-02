from fastapi import Depends, HTTPException
from fastapi_users import FastAPIUsers
from fastapi_users.authentication import AuthenticationBackend, JWTStrategy, CookieTransport
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette import status

from api.auth.manager import get_user_manager
from api.auth.models import User
from api.config.models import Role, RoleAccess
from config import SECRET
from const import ACCESS
from db.session import get_db_general

cookie_transport = CookieTransport(cookie_name="bonds", cookie_secure=False, cookie_samesite="lax",
                                   cookie_max_age=31536000)


def get_jwt_strategy() -> JWTStrategy:
    return JWTStrategy(secret=SECRET, lifetime_seconds=31536000)


auth_backend = AuthenticationBackend(
    name="jwt",
    transport=cookie_transport,
    get_strategy=get_jwt_strategy,
)

fastapi_users = FastAPIUsers[User, int](
    get_user_manager,
    [auth_backend],
)

current_superuser = fastapi_users.current_user(active=True, superuser=True)
current_user = fastapi_users.current_user(active=True, optional=True)


class RoleChecker:

    def __init__(self, required_roles: set[str] = None, required_accesses: set[ACCESS | str] = None) -> None:
        self.required_roles = required_roles
        self.required_accesses = required_accesses

    async def __call__(
            self,
            user: User = Depends(current_user),
            session: AsyncSession = Depends(get_db_general),
    ) -> bool:
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail='not enough permissions'
            )
        user_role = ((await session.execute(select(Role.name).where(Role.id == user.role))).fetchone())[0]

        if self.required_roles:
            if user_role not in self.required_roles:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail='not enough permissions'
                )

        if self.required_accesses:
            role_access = ((await session.execute(select(RoleAccess).where(RoleAccess.role_id == user.role)))
                           .scalars().first())
            if not role_access:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="User role does not have any permissions configured",
                )

            if not any(getattr(role_access, str(access), False) for access in self.required_accesses):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="User does not have required permissions",
                )

        return True
