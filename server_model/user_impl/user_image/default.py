
from __future__ import annotations

from typing import TYPE_CHECKING

from base_model.base_user_modules import IUserImage

if TYPE_CHECKING:
    from .implement import UserImage


class UserImageExtras(IUserImage):
    async def async_get(self: UserImage):
        return {
            'mars_images': await self.async_get_train_images(),
            'user_images': [],
        }
