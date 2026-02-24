from __future__ import annotations
from dataclasses import dataclass

@dataclass
class Builtin:
    ty: str

def builtin_of(x):
    if x in ['int', 'bool', 'float', 'string', 'datetime', 'void']:
        return Builtin(x)

def is_ctor(t):
    return t in ['ref', 'set', 'option', 'record']

@dataclass
class Cons:
    name: str
    params: list[XapiType]

@dataclass
class Class:
    name: str

@dataclass
class Enum:
    name: str

@dataclass
class Opaque:
    pass

XapiType = Builtin | Cons | Enum | Opaque

def parse_type(src: str):
    if src in ['<class> record', 'an event batch']:
        return Opaque()

    src = src.replace('(', ' ( ')
    src = src.replace(')', ' ) ')
    ts = src.split(' ')
    ts = list(filter(lambda x: x != '', ts))
    ts.append('$')

    def lbp(t):
        match t:
            case _ if is_ctor(t):
                return 10
            case '$':
                return -1
            case _:
                return 0

    def shift():
        nonlocal ts
        t = ts[0]
        ts = ts[1:]
        return t

    def peek():
        nonlocal ts
        return ts[0]

    def expect(t):
        curr = shift()
        if curr != t:
            raise Exception(f'Expected {t}, got {curr}')

    def led(left, t):
        if is_ctor(t):
            return Cons(t, [left])
        else:
            raise Exception(f'No left denotation of {t}')

    def nud(t):
        match t:
            case 'enum':
                return Enum(shift())

            case '(':
                l = parse(0)
                expect('->')
                r = parse(0)
                expect(')')
                expect('map')
                return Cons('map', [l, r])

            case _:
                builtin = builtin_of(t)
                if builtin:
                    return builtin
                else:
                    return Class(t)

    def parse(rbp):
        left = nud(shift())
        while lbp(peek()) > rbp:
            left = led(left, shift())
        return left

    try:
        ty = parse(0)
        if peek() == '$':
            return ty
    except e:
        return None
