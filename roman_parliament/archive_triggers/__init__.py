trigger_set = set()


def add_archive_trigger(trigger):
    """
    将trigger加入进trigger_set中，每次父议员重连会调用这个trigger
    :param trigger:
    :return:
    """
    trigger_set.add(trigger)


def run_archive_create_trigger(trigger_name, data):
    for trigger in trigger_set:
        if trigger.__name__ == trigger_name:
            trigger.create_archive(data)
