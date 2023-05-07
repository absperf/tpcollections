import json as _json
from typing import Callable, NamedTuple, Any
from functools import partial
import pickle as _pickle

class Serializer(NamedTuple):
    loads: Callable[[Any], Any]
    dumps: Callable[[Any], Any]

json = Serializer(
    loads=_json.loads,
    dumps=partial(_json.dumps, separators=(',', ':')),
)

pickle = Serializer(
    loads=_pickle.loads,
    dumps=partial(_pickle.dumps, protocol=_pickle.HIGHEST_PROTOCOL),
)

deterministic_json = Serializer(
    loads=_json.loads,
    dumps=partial(_json.dumps, sort_keys=True, separators=(',', ':')),
)

try:
    import orjson as _orjson

    orjson = Serializer(
        loads=_orjson.loads,
        dumps=lambda value: _orjson.dumps(value).decode('utf-8'),
    )
    deterministic_orjson = Serializer(
        loads=_orjson.loads,
        dumps=lambda value: _orjson.dumps(
            value,
            option=_orjson.OPT_SORT_KEYS,
        ).decode('utf-8'),
    )
except ImportError:
    pass
