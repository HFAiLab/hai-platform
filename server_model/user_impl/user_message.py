
from server_model.user import User
from server_model.user_data import UserMessageTable


# Database Messages table
class UserMessage:
    def __init__(self, user: User):
        super().__init__(user)
        self.user: User = user
        self._messages = None

    async def async_get(self):
        await self.fetch_user_messages()
        return self.messages

    def _post_process_df(self, df):
        records = df[df.assigned_to.isin(self.user.group_list)].drop(['assigned_to'], axis='columns').to_dict('records')
        for record in records:
            record['date'] = record['date'].to_pydatetime()
        return records

    async def fetch_user_messages(self) -> None:
        self._messages = self._post_process_df(await UserMessageTable.async_df)

    @property
    def messages(self):
        if self._messages is None:
            self._messages = self._post_process_df(UserMessageTable.df)
        return self._messages
