"""Adding RoleAccess records for existing roles

Revision ID: ebb71f9593ed
Revises: a646b0581238
Create Date: 2024-10-01 17:18:59.387263

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import orm

from api.config.models import Role, RoleAccess

# revision identifiers, used by Alembic.
revision: str = 'ebb71f9593ed'
down_revision: Union[str, None] = 'a646b0581238'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    session = orm.Session(bind=bind)

    existing_roles = session.query(Role).all()

    for role in existing_roles:
        new_access_record = RoleAccess(role_id=role.id)
        session.add(new_access_record)

    session.commit()


def downgrade() -> None:
    bind = op.get_bind()
    session = orm.Session(bind=bind)
    session.query(RoleAccess).delete()
    session.commit()
