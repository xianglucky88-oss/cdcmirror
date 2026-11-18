"""薄编排：loader → normalizer→cdc_reader→checkpoint→switch→verify→order → keyer → store。不含业务判断。

阶段可在记录上置 `_hold` 暂缓落库（通用机制，编排层不解释其业务含义）。"""
import importlib
from . import store as _store
from . import loader as _loader
from .keyer import key_of
from .models import Record

CHAIN_NAMES = ['normalizer', 'cdc_reader', 'checkpoint', 'switch', 'verify', 'order']

def run(con=None):
    own = con is None
    con = con or _store.connect()
    try:
        chain = []
        for n in CHAIN_NAMES:
            mod = importlib.import_module("." + n, __package__)
            chain.append(mod.process)
        fed = 0
        for raw in _loader.load():
            rec = raw
            for fn in chain:
                rec = fn(rec)
            if rec.get("_hold"):
                continue  # 阶段暂缓落库（通用机制）
            _store.upsert(con, Record(key=key_of(rec), data=rec))
            fed += 1
        _store.commit(con)
        return {"fed": fed, "rows": _store.count(con)}
    finally:
        if own:
            con.close()
