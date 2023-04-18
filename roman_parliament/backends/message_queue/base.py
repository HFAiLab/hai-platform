
class BaseMQ:
    @classmethod
    def send_channel(cls, data, channel):
        raise NotImplementedError

    @classmethod
    def listen_channel(cls, channel):
        raise NotImplementedError

    def __init__(self, channel):
        self.channel = channel

    def send(self, data):
        return self.__class__.send_channel(data=data, channel=self.channel)

    def listen(self):
        yield from self.__class__.listen_channel(channel=self.channel)
