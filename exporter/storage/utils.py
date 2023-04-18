

collectors = []

# 支持的 column
SUPPORTED_COLUMNS = [
    'used_bytes',       # 精确的用量
    'limit_bytes',      # quota
    'fetched_data',     # 0或1代表是否获取到了精确数据, 无此数据时默认为 1, optional, 目前只有 weka 的部分目录会没有精确数据
    'unknown_bytes',    # 当前目录下未知归属的用量, optional, 目前只有 weka 使用
    'sum_bytes',        # 当前目录下用量总和, 用于在获取不到精确数据时代替精确用量, optional, 目前只有 weka 使用
]
SUPPORTED_LABEL = [
    'tag',
    'user_name',     # 目前只有 weka 提供该 tag
]


def register_collector(measurement, columns=None, labels=None):
    """
    注册存储用量收集函数, 该函数不接收任何参数, 返回一个字典的列表, 每个元素描述一个路径的用量信息.
    Schema 参考:
    [{
        'host_path': '/weka-jd/prod',
        'column_1': value,
        'column_2': value,
        'label_1': 'string value',
    }]
    """

    columns = columns or []
    labels = labels or []
    assert all(column in SUPPORTED_COLUMNS for column in columns)
    assert all(label in SUPPORTED_LABEL for label in labels)

    def decorator(func):
        collectors.append((measurement, func, columns, labels))
        return func
    return decorator
