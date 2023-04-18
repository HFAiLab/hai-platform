
from typing import Optional

import munch

from server_model.user_data import TrainEnvironmentTable


class AioTrainEnvironmentSelector:
    @classmethod
    async def find_one(cls, env_name) -> Optional[munch.Munch]:
        df = await TrainEnvironmentTable.async_df
        df = df[df.env_name == env_name]
        if len(df) > 0:
            return munch.Munch.fromDict(df.iloc[0].to_dict())
        return None

    @classmethod
    async def find_all(cls):
        return (await TrainEnvironmentTable.async_df).to_dict('records')
