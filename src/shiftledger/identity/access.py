from dataclasses import dataclass
from uuid import UUID

from shiftledger.identity.models import Role


@dataclass(frozen=True)
class MemberContext:
    organization_id: UUID
    user_id: UUID
    role: Role

    @property
    def is_admin(self) -> bool:
        return self.role == Role.ADMIN
