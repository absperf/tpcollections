from typing import Union

class Identifier:
    '''An auto-escaping identifier similar to a string.

    Conceptually immutable.
    '''
    __slots__ = (
        '__value',
    )

    def __init__(self, value: Union['Identifier', str] = "") -> None:
        if isinstance(value, Identifier):
            value = value.value

        self.__value = value

    @property
    def value(self) -> str:
        return self.__value

    def __add__(self, other: Union['Identifier', str]) -> 'Identifier':
        if isinstance(other, Identifier):
            other = other.__value
        return Identifier(self.__value + other)

    def __radd__(self, other: Union['Identifier', str]) -> 'Identifier':
        if isinstance(other, Identifier):
            other = other.__value
        return Identifier(other + self.__value)

    def __contains__(self, other: Union['Identifier', str]) -> bool:
        if isinstance(other, Identifier):
            other = other.__value
        return self.__value.__contains__(other)

    def __hash__(self) -> int:
        return hash(self.__value)

    def __repr__(self) -> str:
        return f'<Identifier {self.__value!r}>'

    def __str__(self) -> str:
        if b'\x00' in self.__value.encode('utf-8'):
            raise ValueError("sqlite Identifer must not contain any null bytes")

        return '"' + self.__value.replace('"', '""') + '"'
