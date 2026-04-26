import pathlib
import yaml
from functools import cache
from collections.abc import Mapping


DIR_RESOURCES = pathlib.Path(__file__).parent
FILE_DEPTS = DIR_RESOURCES / 'departments.yml'
FILE_ROLES = DIR_RESOURCES / 'roles.yml'


class FrozenDict(Mapping):
    def __init__(self, **kwargs):
        self.__data = {}
        for key, value in kwargs.items():
            if hasattr(value, "__iter__") and not isinstance(value, str):
                value = tuple(value)
            self.__data[key] = value

    def __getitem__(self, key):
        return self.__data[key]

    def __iter__(self):
        return iter(self.__data)

    def __len__(self):
        return len(self.__data)
    
    def items(self):
        return self.__data.items()
    
    def keys(self):
        return self.__data.keys()
    
    def values(self):
        return self.__data.values()
    
    def __repr__(self):
        items = ", ".join(f"{k}={v!r}" for k, v in self.__data.items())
        return f"FrozenDict({items})"
    
    def __str__(self):
        items = ", ".join(f"{k}={v}" for k, v in self.__data.items())
        return f"FrozenDict({items})"
    
    def __hash__(self):
        items = self.__data.items()
        sorted_items = sorted(items, key=lambda x: x[0])
        tuple_items = tuple(sorted_items)
        return hash(tuple_items)
    
    def __eq__(self, other):
        if not isinstance(other, FrozenDict):
            return NotImplemented
        return self.__data == other.__data


@cache
def departments():
    with open(FILE_DEPTS) as f:
        return FrozenDict(**yaml.safe_load(f))
    

@cache
def roles():
    with open(FILE_ROLES) as f:
        return FrozenDict(**yaml.safe_load(f))
    

@cache
def checklist_for(map: FrozenDict) -> list[tuple[str, str]]:
    assert map is not None
    checkmap = {}
    for key, values in map.items():
        for value in values:
            if value in checkmap:
                raise ValueError(f"Duplicate value in checklist: {value!r} (keys: {checkmap[value]!r} and {key!r})")
            checkmap[value] = key
    checklist = list(checkmap.items())
    checklist.sort(key=lambda x: len(x[0]), reverse=True)
    return checklist


def classify_role(role: str) -> str | None:
    role = role.lower().strip()
    for value, key in checklist_for(roles()):
        if value in role:
            return key
    return None


def classify_dept(dept: str) -> str | None:
    dept = dept.lower().strip()
    for value, key in checklist_for(departments()):
        if value in dept:
            return key
    return None
