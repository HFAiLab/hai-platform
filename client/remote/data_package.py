from zipfile import ZipFile
import pickle as pkl


def save(data, file_path):
    """
    data 中有无法 pkl 的数据，不直接 pkl 整个 data，而是 pkl 成 key.pkl，
        然后将其 zip 起来；无法 pkl 的我们就给出提示；不直接挂掉
    :param data: dict or list or tuple
    :param file_path: 返回
    :return:
    """
    data_type = type(data)
    assert data_type in [tuple, list, dict], f'只能保存 tuple, list, dict, 输入为 {data_type}'
    if data_type in [tuple, list]:
        dict_data = {str(i): data[i] for i in range(len(data))}
    else:
        dict_data = data
    dict_data['__data_type__'] = data_type.__name__
    with ZipFile(file_path, 'w') as data_zip:
        for k in dict_data:
            try:
                pkl_bytes = pkl.dumps(dict_data[k])
                data_zip.writestr(k, pkl_bytes)
            except:
                print(f'不能 pickle 保存参数 {k}, 类型 {type(dict_data[k])}，'
                      f'跳过这项，无法在 remote call 中传递，在使用中需要用户自己处理')
    del dict_data['__data_type__']  # 避免污染 dict_data


def load(file_path):
    """
    load save 下去的 dp 文件

    :param file_path:
    :return: list, tuple, dict
    """
    dict_data = {}
    with ZipFile(file_path) as data_zip:
        for k in data_zip.filelist:
            with data_zip.open(k.filename) as kf:
                dict_data[k.filename] = pkl.loads(kf.read())

    data_type = dict_data.get('__data_type__', None)
    del dict_data['__data_type__']
    assert data_type in ['dict', 'list', 'tuple'], f'保存的数据必须为[dict, list, tuple]，当前为: {data_type}'

    if data_type == 'dict':
        return dict_data

    list_data = []
    for i in range(len(dict_data)):
        list_data.append(dict_data[str(i)])
    if data_type == 'list':
        return list_data
    # elif data_type == 'tuple':
    return tuple(list_data)