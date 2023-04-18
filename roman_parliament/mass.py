from collections import defaultdict


_key_list = []
_mass_key_dict = {}
_mass_name = None
_mass_set = defaultdict(set)


def set_mass_info(key_list, mass_name):
    """
    :param key_list: List[str]，manager要求订阅的所有档案key
    :param mass_name: mass对应的唯一名
    :return:
    """
    global _key_list
    global _mass_name
    _key_list = [key if key.startswith('registered') else f'registered_{key}' for key in key_list]
    _mass_name = mass_name


def get_mass_info():
    return _key_list, _mass_name


def record_mass(key_list, mass_name):
    global _mass_set
    global _mass_key_dict
    for key in key_list:
        _mass_set[key].add(mass_name)
    _mass_key_dict[mass_name] = key_list


def remove_mass(mass_name):
    global _mass_set
    global _mass_key_dict
    key_list = _mass_key_dict.pop(mass_name, [])
    for key in key_list:
        try:
            _mass_set[key].remove(mass_name)
            if _mass_set[key] == set():
                _mass_set.pop(key)
        except:
            pass


def get_mass_set():
    return _mass_set
