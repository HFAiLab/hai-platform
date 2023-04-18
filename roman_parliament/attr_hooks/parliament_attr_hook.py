from .base_hook import BaseHook
from roman_parliament.backends import backend


class ParliamentAttrType:
    ATTR = 'attr'
    INDEX = 'index'


class ParliamentAttrMetaType:
    def __init__(self, source, data, next_layer=None, value=None, timestamp=None):
        self.source = source
        self.data = data
        self.next_layer = next_layer
        self.value = value
        self.timestamp = timestamp  # 是None说明不是转发来的

    def __str__(self):  # 打印用，可以用来简单debug
        if self.next_layer is not None:
            print(self.next_layer)
        return f'source: {self.source}; data: {self.data}; value: {self.value}; timestamp: {self.timestamp}'


def get_exp_from_parliament_attr(value):
    if value.source == ParliamentAttrType.ATTR:
        return f'.{value.data}'
    if value.source == ParliamentAttrType.INDEX:
        return f"['{value.data}']" if isinstance(value.data, str) else f"[{value.data}]"


def go_next_layer(instance, value):
    if value.source == ParliamentAttrType.ATTR:
        return getattr(instance, value.data)
    if value.source == ParliamentAttrType.INDEX:
        return instance[value.data]


def update_instance(instance, value):
    if value.source == ParliamentAttrType.ATTR:
        setattr(instance, value.data, value.value)
    if value.source == ParliamentAttrType.INDEX:
        instance[value.data] = value.value


class ParliamentAttrHook(BaseHook):
    @classmethod
    def set_attr(cls, description, instance, value: ParliamentAttrMetaType):
        recurrence_instance = instance
        recurrence_value = value
        exp = ''
        while recurrence_value.next_layer is not None:
            exp += get_exp_from_parliament_attr(recurrence_value)
            recurrence_instance = go_next_layer(recurrence_instance, recurrence_value)
            recurrence_value = recurrence_value.next_layer
        exp += get_exp_from_parliament_attr(recurrence_value)  # 获取最后一层表达式，exp就是要真正执行的表达式
        # todo: 如果判断条件多了，可以作成instance.apply(exp)作为类的方法
        if exp.startswith('.pods') and exp.endswith('.status'):  # XX.pods[pod_id].status = XXX
            if recurrence_value.timestamp is None:  # 这说明是数据的创造者
                try:
                    result = recurrence_instance.update(('status', ), (recurrence_value.value, ))  # 落地到数据库里，由数据库来控制状态机
                    timestamp = result.fetchone()[0].timestamp()  # 数据库落地成功
                    update_instance(recurrence_instance, recurrence_value)  # 该内存里的数据
                    recurrence_value.timestamp = timestamp  # 接受到信息里有timestamp的就一定是接受者
                    setattr(instance, exp, timestamp)  # 设置timestamp
                    return True
                except Exception as e:  # 数据库落地失败，直接退出
                    return False
            else:  # 这说明是数据的接受者（转发者），不作转发
                if hasattr(instance, exp) and recurrence_value.timestamp < getattr(instance, exp):  # 不如之前的更新
                    return False
                update_instance(recurrence_instance, recurrence_value)  # 该内存里的数据
                setattr(instance, exp, recurrence_value.timestamp)
                return False
        return True

    @classmethod
    def broadcast_data(cls, data, setting_result):
        if setting_result:  # setting_result为True才转发这条消息
            backend.set(data, mass=True)

    @classmethod
    def update_attr(cls, archive, data):
        # 对于parliament_attr, 当接收端发现数据更新时，再走一遍set_attr流程，但不作转发
        setattr(archive, data['attr'][len('real_'):], data['value'])


def generate_parliament_attr_value(exp, value):
    """
    把表达式赋值的语句转化成可以被pickle的实例
    :param exp: 例如 .status['1234'][1].pod["abc"].new
    :param value:
    :return:
    """
    assert exp.startswith('.') or exp.startswith('['), '请重新看下格式'
    root_instance = ParliamentAttrMetaType('', '')
    current_instance = root_instance
    while True:
        next_dot_pos, next_bracket_pos = exp[1:].find('.'), exp[1:].find('[')
        source = ParliamentAttrType.ATTR if exp[0] == '.' else ParliamentAttrType.INDEX
        next_pos = max(next_dot_pos, next_bracket_pos) if next_dot_pos == -1 or next_bracket_pos == -1 else min(next_dot_pos, next_bracket_pos)
        next_pos = len(exp) if next_pos == -1 else next_pos + 1
        if source == ParliamentAttrType.ATTR:
            data = exp[1: next_pos]
        if source == ParliamentAttrType.INDEX:  # 去掉右括号
            data = exp[1: next_pos - 1]
            data = data.strip('"\'') if data.startswith('\'') or data.startswith('"') else int(data)
        if next_pos == len(exp):  # 要结束了
            current_instance.next_layer = ParliamentAttrMetaType(source=source, data=data, value=value)
            return root_instance.next_layer
        else:
            # 找到下一个最近的位置
            current_instance.next_layer = ParliamentAttrMetaType(source=source, data=data)
            exp = exp[next_pos:]
            current_instance = current_instance.next_layer
