
from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .implement import UserQuota


class UserQuotaExtras:
    def basic_quota(self: UserQuota):
        return {
            'port_quota': self.port_quota,
            'jupyter_quota': self.jupyter_quota,
            'train_environments': self.train_environments,
        }
