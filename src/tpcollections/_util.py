from typing import Optional, Union

class Identifier:
    '''An auto-escaping identifier similar to a string.

    Conceptually immutable.
    '''
    __slots__ = (
        '__value',
        '__safe',
    )

    def __init__(self, value: Union['Identifier', str] = "") -> None:
        self.__safe: Optional[str]
        if isinstance(value, Identifier):
            self.__value = value.__value
            self.__safe = value.__safe
        else:
            self.__value = value
            self.__safe = None

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
        if self.__safe is None:
            if b'\x00' in self.__value.encode('utf-8'):
                raise ValueError("sqlite Identifer must not contain any null bytes")

            self.__safe = '"' + self.__value.replace('"', '""') + '"'

        return self.__safe
