
from typing import Optional

import munch

from server_model.user_data import TrainEnvironmentTable


class TrainEnvironmentSelector:
    @classmethod
    def find_one(cls, env_name) -> Optional[munch.Munch]:
        df = TrainEnvironmentTable.df
        df = df[df.env_name == env_name]
        if len(df) > 0:
            return munch.Munch.fromDict(df.iloc[0].to_dict())
        return None
