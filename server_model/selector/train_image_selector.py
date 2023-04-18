
from typing import Optional

import munch
import pandas as pd

from server_model.user_data import TrainImageTable


class TrainImageSelector:
    @classmethod
    def _convert_to_datetime(cls, df: pd.DataFrame):
        if len(df) == 0:
            return df
        df.created_at = pd.Series(df.created_at.dt.to_pydatetime(), index=df.index, dtype='object')
        df.updated_at = pd.Series(df.updated_at.dt.to_pydatetime(), index=df.index, dtype='object')
        return df

    @classmethod
    def add_url(cls, image: munch.Munch):
        if image.get('registry') and image.get('shared_group') and image.get('image'):
            image.image_url = '/'.join([image.registry, image.shared_group, image.image])
        return image

    @classmethod
    def find_one(cls, image) -> Optional[munch.Munch]:
        df = TrainImageTable.df
        df = df[df.image == image]
        if len(df) > 0:
            return cls.add_url(munch.Munch.fromDict(df.iloc[0].to_dict()))
        return None

    @classmethod
    async def a_find_one(cls, shared_group, image: str = None, image_tar: str = None) -> Optional[munch.Munch]:
        df = await TrainImageTable.async_df
        df = df[df.shared_group == shared_group]
        df = df[df.image == image] if image is not None else df
        df = df[df.image_tar == image_tar] if image_tar is not None else df
        if len(df) > 0:
            return cls.add_url(munch.Munch.fromDict(df[df.updated_at == df.updated_at.max()].iloc[0].to_dict()))
        return None

    @classmethod
    async def a_find_user_group_images(cls, shared_group: str):
        df = await TrainImageTable.async_df
        df = df[df.shared_group == shared_group].sort_values('updated_at')
        return cls._convert_to_datetime(df).to_dict('records')

    @classmethod
    async def a_find_user_group_image_urls(cls, shared_group: str, status: str = None):
        df = await TrainImageTable.async_df
        df = df[df.shared_group == shared_group]
        df = df[df.status == status] if status is not None else df
        if len(df) == 0:
            return []
        return (df.registry + '/' + df.shared_group + '/' + df.image).tolist()
