import argparse
from dataclasses import dataclass
import json
from xapitypes import *

preamble = """
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, ClassVar

import requests
import json
import uuid

def _id(x):
  return x

def parse_date(s):
    return datetime.strptime(s, "%Y%m%dT%H:%M:%SZ")

def encode_date(d):
    return d.strftime('%Y%m%dT%H:%M:%SZ')

@dataclass
class Ref[T]:
    ref: str
    NULL: ClassVar['Ref[object]']
    'Null reference'

    def __hash__(self):
        return self.ref.__hash__()

    def __bool__(self):
        return self.ref != 'OpaqueRef:NULL'

Ref.NULL = Ref('OpaqueRef:NULL')

def ref_of(s):
    return Ref(s)

def encode_ref(r):
    return r.ref

class Connection:
    def __init__(self, endpoint):
      self.endpoint = endpoint
      self.session = Ref.NULL

    def call(self, fn, ps):
        rpc = {
            'jsonrpc': '2.0',
            'method': fn,
            'params': ps,
            'id': str(uuid.uuid4())
        }

        hdr = {
            'Content-type': 'application/json'
        }

        url = f'{self.endpoint}/jsonrpc'
        res = requests.post(url, json=rpc).json()
        if 'error' in res:
            msg = res['error']['message']
            exc = _exception_tbl.get(msg)
            raise (exc if exc is not None else Exception(msg))
        return res.get('result')
"""

async_preamble = """
import aiohttp
import asyncio

class AsyncConnection:
    def __init__(self, endpoint, timeout_seconds=10):
        self.endpoint = endpoint
        self.session = None
        self.client = None

    async def __aenter__(self):
        self.client = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.client:
            await self.client.close()

    async def call(self, fn, ps, **kwargs):
        rpc = {
            'jsonrpc': '2.0',
            'method': fn,
            'params': ps,
            'id': str(uuid.uuid4())
        }

        hdr = {
            'Content-Type': 'application/json'
        }
        url = f'{self.endpoint}/jsonrpc'

        timeout = aiohttp.ClientTimeout(total=kwargs.get('timeout', 30))
        async with self.client.post(url, json=rpc, headers=hdr, timeout=timeout) as response:
            response.raise_for_status()
            res = await response.json()

        if 'error' in res:
            msg = res['error']['message']
            exc = _exception_tbl.get(msg)
            raise exc(msg) if exc else Exception(msg)

        return res.get('result')
"""

@dataclass
class Config:
    is_async: bool

config = Config(is_async=False)

def p(*args, **kwargs):
    if len(args) >= 2 and isinstance(args[0], int):
        c = int(args[0])
        args = args[1:]
        args = ((' ' * c) + args[0],) + args[1:]
    print(*args, **kwargs)

@dataclass
class XapiField:
    name: str
    description: str
    ty: XapiType

@dataclass
class XapiEnum:
    name: str
    values: list[tuple[str, str]] # [(name, doc)]

@dataclass
class XapiParam:
    name: str
    ty: XapiType
    description: str
    required: bool

@dataclass
class XapiMessage:
    name: str
    params: list[XapiParam]
    result: XapiType
    description: str
    errors: list[tuple[str, str]] # [(name, doc)]

@dataclass
class XapiObject:
    name: str
    description: str
    fields: list[XapiField]
    enums: list[XapiEnum]
    messages: list[XapiMessage]

def parse_field(f):
    name = f['name']
    description = f['description']
    ty = parse_type(f['type'])
    return XapiField(name, description, ty)

def parse_enum(e):
    name = rename_class(e['name'])
    values = [(v['name'], v['doc']) for v in e['values']]
    return XapiEnum(name, values)

def parse_param(p):
    name = p['name']
    ty = parse_type(p['type'])
    description = p['doc']
    required = p['required']
    return XapiParam(name, ty, description, required)

def parse_message(m):
    name = m['name']
    params = [parse_param(p) for p in m['params']]
    result = parse_type(m['result'][0])
    errors = [(v['name'], v['doc']) for v in m['errors']]
    description = m['description']
    return XapiMessage(name, params, result, description, errors)

def parse_object(o):
    name = o['name']
    description = o['description']
    fields = [parse_field(f) for f in o['fields']]
    enums = [parse_enum(e) for e in o['enums']]
    messages = [parse_message(m) for m in o['messages']]
    return XapiObject(name, description, fields, enums, messages)

def rename_class(c):
    def up(p):
        if not p.isupper():
            return p.capitalize()
        else:
            return p
    return ''.join(up(p) for p in c.split('_'))

def rename_field(f):
    f = f.lower()
    return {'class': 'clazz'}.get(f, f)

def rename_builtin(n):
    return {
        'string': 'str',
        'void': 'None',
    }.get(n, n)

def rename_ctor(n):
    return {
        'ref': 'Ref',
        'map': 'dict',
    }.get(n, n)

def string_of_type(ty):
    match ty:
        case Builtin(n):
            return rename_builtin(n)

        case Cons('record', [t]):
            return f'{string_of_type(t)}.Record'

        case Cons('option', [t]):
            return f'{string_of_type(t)} | None'

        case Cons(n, ps):
            ps = [string_of_type(p) for p in ps]
            ps = ','.join(ps)
            n = rename_ctor(n)
            return f'{n}[{ps}]'

        case Class(n) | Enum(n):
            return rename_class(n)

    return 'Any'

def unmarshaller():
    c = -1
    def gensym(p):
        nonlocal c
        c += 1
        return f'{p}{c}'

    def go(ty: XapiType, k):
        match ty:
            case Builtin('datetime'):
                return k('parse_date')

            case Builtin(ty):
                return k('_id')

            case Opaque():
                return k('_id')

            case Enum(n):
                g = gensym('g')
                x = gensym('x')
                ls = k(g)
                ctor = rename_class(n)
                tr = f'{g} = lambda {x}: {ctor}({x})'
                return [tr] + ls

            case Cons('record', [Class(clazz)]):
                return k(f'{rename_class(clazz)}.unmarshal')

            case Cons('option', [e]):
                g = gensym('g')
                x = gensym('x')
                ls = k(g)
                tr = f'{g} = lambda {x}: {x}'
                return [tr] + ls

            case Cons('set', [e]):
                def cont(et):
                    nonlocal k
                    g, x, ev = (gensym(n) for n in ('g', 'x', 'e'))
                    tr = f'{g} = lambda {x}: set([{et}({ev}) for {ev} in {x}])'
                    return [tr] + k(g)
                return go(e, cont)

            case Cons('map', [l, r]):
                def c0(lt):
                    def c1(rt):
                        nonlocal k
                        g, x, kv, vv = (gensym(n) for n in ('g', 'x', 'k', 'v'))
                        tr = f'{g} = lambda {x}: {{ {lt}({kv}): {rt}({vv}) for {kv}, {vv} in {x}.items() }}'
                        return [tr] + k(g)
                    return go(r, c1)
                return go(l, c0)

            case Cons('ref', [ty]):
                return k('ref_of')

    return go

def escape(s):
    s = s.replace("'", "\\'")
    return s

def emit_nested_record(obj):
    if not obj.fields:
        return

    p(4, '@dataclass')
    p(4, 'class Record:')
    # declare all the fields
    for f in obj.fields:
        fn = rename_field(f.name)
        fty = string_of_type(f.ty)
        desc = f.description.replace("'", "\\'")
        p(8, f'{fn}: {fty}')
        p(8, f"'{desc}'")
    p()

    # emit the unmarshalling routine
    # return Record(uuid=p0, ...=p1)
    parsed = []
    rty = f'{rename_class(obj.name)}.Record'

    # emit unmarshalling routine, dict -> record
    p(4, '@staticmethod')
    p(4, 'def unmarshal(o):')
    um = unmarshaller()
    for i, f in enumerate(obj.fields):
        def go(tr):
            nonlocal i, f
            return [f"p{i} = {tr}(o['{f.name}'])"]
        for l in um(f.ty, go):
            p(8, l)

    p(8, f'return {rty}(', end='')
    for i, f in enumerate(obj.fields):
        p(f'{rename_field(f.name)} = p{i}', end='')
        if i < len(obj.fields) - 1:
            p(', ', end='')
    p(')\n')

    # emit marshalling routine, record -> dict
    p(4, '@staticmethod')
    p(4, f'def marshal(r: {rty}):')
    p(8, 'd = dict()')
    m = marshaller()
    for f in obj.fields:
        def after(tr):
            return [f"d['{f.name}'] = {tr}(r.{rename_field(f.name)})"]
        for l in m(f.ty, after):
            p(8, l)
    p(8, 'return d\n')

def rename_param(n):
    return {
        'class': 'clazz',
        'timeout': '_timeout',
    }.get(n, n)

def rename_message(n):
    return {
        'from': '_from',
        'import': '_import',
    }.get(n, n).lower()

def remove_session(ps):
    match ps:
        case [XapiParam('session_id', Cons('ref', [Class('session')])), *rest]:
            return True, rest
        case _:
            return False, ps

def find_first_optional(ps):
    for i, p in enumerate(ps):
        if not p.required:
            return i
    return len(ps)

def marshaller():
    c = -1
    def gensym(p):
        nonlocal c
        c += 1
        return f'{p}{c}'

    def go(ty: XapiType, k):
        match ty:
            case Builtin('datetime'):
                return k('encode_date')

            case Builtin(ty):
                return k('_id')

            case Opaque():
                return k('_id')

            case Cons('ref', [ty]):
                return k('encode_ref')

            case Enum(n):
                g = gensym('j')
                x = gensym('x')
                ls = k(g)
                tr = f'{g} = lambda {x}: {x}.value'
                return [tr] + ls

            case Cons('option', [e]):
                g = gensym('g')
                x = gensym('x')
                ls = k(g)
                tr = f'{g} = lambda {x}: {x}'
                return [tr] + ls

            case Cons('record', [Class(clazz)]):
                return k(f'{rename_class(clazz)}.marshal')

            case Cons('set', [e]):
                def cont(et):
                    nonlocal k
                    g, x, ev = (gensym(n) for n in ('j', 'x', 'e'))
                    tr = f'{g} = lambda {x}: [{et}({ev}) for {ev} in {x}]'
                    return [tr] + k(g)
                return go(e, cont)

            case Cons('map', [l, r]):
                def c0(lt):
                    def c1(rt):
                        nonlocal k
                        g, x, kv, vv = (gensym(n) for n in ('j', 'x', 'k', 'v'))
                        tr = f'{g} = lambda {x}: {{ {lt}({kv}): {rt}({vv}) for {kv}, {vv} in {x}.items() }}'
                        return [tr] + k(g)
                    return go(r, c1)
                return go(l, c0)

    return go

def emit_message(obj, m):
    global config
    mn = rename_message(m.name)
    rty = string_of_type(m.result)

    has_session, params = remove_session(m.params)
    partition = find_first_optional(params)
    required = params[:partition]
    optional = params[partition:]

    cty = 'Async' if config.is_async else ''
    cty += 'Connection'

    prefix = 'async ' if config.is_async else ''

    p(4, '@staticmethod')
    p(4, f'{prefix}def {mn}(conn: {cty}', end='')
    if params:
        p(', ', end='')
        for i, pa in enumerate(params):
            pn = rename_param(pa.name)
            pty = string_of_type(pa.ty)
            if not pa.required:
                pty += ' | None = None'
            p(f'{pn}: {pty}', end='')
            if i < len(params) - 1:
                p(', ', end='')

    # accept keyword args if async
    if config.is_async:
        p(', **kwargs', end='')
    p(f') -> {rty}:')

    # emit doc string
    p(8, f'"""{m.description}"""')

    # start construction of parameter list
    p(8, f'_ps = {"[]" if not has_session else "[conn.session.ref]"}')

    # required arguments are marshalled and appended
    ma = marshaller()
    for r in required:
        def after(tr):
            return [f'_ps.append({tr}({rename_param(r.name)}))']
        for l in ma(r.ty, after):
            p(8, l)

    # optional arguments are marshalled
    # and then appended to the parameter list so long as they are non-None
    if optional:
        trs = []
        for o in optional:
            def after(tr):
                trs.append(tr)
                return []
            for l in ma(o.ty, after):
                p(8, l)
        p(8, f'_trs : list[Any] = [{",".join(trs)}]')
        opts = ','.join([rename_param(p.name) for p in optional])
        p(8, f'_opts : list[Any] = [{opts}]')
        p(8, 'for _i, _o in enumerate(_opts):')
        p(12, 'if _o is None:')
        p(16, 'break')
        p(12, f'_ps.append(_trs[_i](_o))')
        p()

    suffix = ', **kwargs' if config.is_async else ''
    prefix = 'await ' if config.is_async else ''
    p(8, f"_res = {prefix}conn.call('{obj.name}.{m.name}', _ps{suffix})")

    # special casing of messages that modify connection state
    match m.name:
        case 'login_with_password':
            p(8, 'conn.session = Ref(_res)')
        case 'logout':
            p(8, 'conn.session = Ref.NULL')

    # unmarshal the result
    match m.result:
        case Builtin('void'):
            p(8, 'return None')
        case _:
            um = unmarshaller()
            ls = um(m.result, lambda tr: [f'return {tr}(_res)'])
            for l in ls:
                p(8, l)
    p()

def emit_messages(obj):
    for m in obj.messages:
        emit_message(obj, m)

def emit_objects(objs):
    for o in objs:
        name = rename_class(o.name)
        if name == 'Auth':
            continue
        p(f'class {name}:')
        desc = escape(o.description)
        p(4, f"'''{desc}'''")
        emit_nested_record(o)
        emit_messages(o)
        p()

def emit_enums(objs: [XapiObject]):
    seen = set()

    def emit_enum(e: XapiEnum):
        p(f'class {e.name}(Enum):')
        for (name, doc) in e.values:
            name = name.replace('-', '_')
            doc = escape(doc)
            p(4, f"'{doc}'")
            p(4, f"{name.upper()} = '{name}'")
        p()

    for o in objs:
        for e in o.enums:
            if e.name not in seen:
                emit_enum(e)
            seen.add(e.name)

def rename_exception(n):
    ps = n.lower().split('_')
    return ''.join(p.capitalize() for p in ps)

def emit_exceptions(objs: [XapiObject]):
    seen = set()

    def emit_exception(name, doc):
        name = rename_exception(name)
        doc = escape(doc)
        p(f'class {name}(Exception):')
        p(4, f"'{doc}'")
        p(4, f"def __init__(self, msg='{doc}'):")
        p(8, f'super().__init__(msg)')
        p()

    for o in objs:
        for m in o.messages:
            for e in m.errors:
                n, d = e
                if n not in seen:
                    emit_exception(n, d)
                    seen.add(n)

    p('_exception_tbl = {')
    for e in sorted(seen):
        p(4, f"'{e}': {rename_exception(e)},")
    p('}\n')

def parse_args():
    p = argparse.ArgumentParser(description='Emit typed bindings for XenAPI')
    p.add_argument(
        'input_file',
        type=str,
        help='Path to xenapi.json'
    )
    p.add_argument(
        "--is-async",
        action='store_true',
        default=False,
        help='Emit async version of bindings'
    )
    return p.parse_args()

def main():
    global config
    args = parse_args()
    config.is_async = args.is_async
    f = open(args.input_file, 'r')
    objs = json.load(f)
    f.close()
    objs = [parse_object(o) for o in objs]
    p(preamble)
    if config.is_async:
        p(async_preamble)
    emit_enums(objs)
    emit_exceptions(objs)
    emit_objects(objs)

if __name__ == '__main__':
    main()
