
from types import SimpleNamespace


def _get_dict_value(_obj, _fields):
    if _obj is None:
        return SimpleNamespace(exist=False, value=None, error=None)
    if not isinstance(_obj, dict):
        return SimpleNamespace(exist=False, value=None, error="not_a_dict")

    if len(_fields) == 1:
        exist = _fields[0] in _obj
        value = _obj.get(_fields[0])
        return SimpleNamespace(exist=exist, value=value, error=None)
    else:
        return _get_dict_value(_obj.get(_fields[0]), _fields[1:])


def get_dict_value(obj, field):
    """Get a dict value from obj

    field can be in the form "listing.price"

    return SimpleNamespace which contains exist, value and error
    """
    fields = field.split(".")
    return _get_dict_value(obj, fields)
