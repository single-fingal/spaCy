from typing import Iterable, Tuple, Union, Optional, TYPE_CHECKING
import warnings
import weakref
from collections import UserDict
import srsly

from .span_group import SpanGroup
from ..errors import Errors, Warnings


if TYPE_CHECKING:
    # This lets us add type hints for mypy etc. without causing circular imports
    from .doc import Doc  # noqa: F401
    from .span import Span  # noqa: F401


# Why inherit from UserDict instead of dict here?
# Well, the 'dict' class doesn't necessarily delegate everything nicely,
# for performance reasons. The UserDict is slower but better behaved.
# See https://treyhunner.com/2019/04/why-you-shouldnt-inherit-from-list-and-dict-in-python/0ww
class SpanGroups(UserDict):
    """A dict-like proxy held by the Doc, to control access to span groups."""

    _EMPTY_BYTES = srsly.msgpack_dumps([])

    def __init__(
        self, doc: "Doc", items: Iterable[Tuple[str, SpanGroup]] = tuple()
    ) -> None:
        self.doc_ref = weakref.ref(doc)
        UserDict.__init__(self, items)  # type: ignore[arg-type]

    def __setitem__(self, key: str, value: Union[SpanGroup, Iterable["Span"]]) -> None:
        if not isinstance(value, SpanGroup):
            value = self._make_span_group(key, value)
        assert value.doc is self.doc_ref()
        UserDict.__setitem__(self, key, value)

    def _make_span_group(self, name: str, spans: Iterable["Span"]) -> SpanGroup:
        doc = self._ensure_doc()
        return SpanGroup(doc, name=name, spans=spans)

    def copy(self, doc: Optional["Doc"] = None) -> "SpanGroups":
        if doc is None:
            doc = self._ensure_doc()
        return SpanGroups(doc).from_bytes(self.to_bytes())

    def to_bytes(self) -> bytes:
        # We should serialize this as a dict, even though groups know their names:
        # if a group has the same .name as another group in `self`,
        # we need to know which key will map to which group. (See #10685)
        if len(self) == 0:
            return self._EMPTY_BYTES

        # Since SpanGroup objects can currently be the value of multiple `SpanGroups` keys,
        # we serialize each SpanGroup object once.
        # Then, upon deserialization, any SpanGroup that happened to be a value
        # for more than 1 key will be present only once in the resulting `SpanGroups`.
        id_bytes_keys = {}
        for key, value in self.items():
            value_id = id(value)
            if value_id not in id_bytes_keys:
                id_bytes_keys[value_id] = [value.to_bytes(), key]
            else:
                id_bytes_keys[value_id].append(key)
        msg = {value_bytes: keys for value_bytes, *keys in id_bytes_keys.values()}
        return srsly.msgpack_dumps(msg)

    def from_bytes(self, bytes_data: bytes) -> "SpanGroups":
        self.clear()
        doc = self._ensure_doc()
        # backwards-compatibility: bytes_data may be one of:
        # b'', a serialized empty list, a serialized list of SpanGroup bytes,
        # (and now it may be) a serialized dict mapping keys to SpanGroup bytes
        if bytes_data and bytes_data != self._EMPTY_BYTES:
            msg = srsly.msgpack_loads(bytes_data)
            if isinstance(msg, dict):
                # The 2nd version of `SpanGroups` serialization
                for value_bytes, keys in msg.items():
                    group = SpanGroup(doc).from_bytes(value_bytes)
                    for key in keys:
                        self[key] = group
            elif isinstance(msg, list):
                # The 1st version of `SpanGroups` serialization
                for value_bytes in msg:
                    group = SpanGroup(doc).from_bytes(value_bytes)
                    group_name = group.name
                    if group_name in self:
                        # Display a warning if `msg` contains `SpanGroup`s
                        # that have the same .name (attribute).
                        # Because, for `SpanGroups` serialized as lists,
                        # only 1 SpanGroup per .name is loaded. (See #10685)
                        warnings.warn(Warnings.W119.format(group_name=group_name))
                    self[group_name] = group
            # TODO: Raise exception if `msg` is neither dict nor list?
            #       (Or should we just use `else` instead of the `elif` above
            #        (and quietly ignore any (invalid) non-dict/list data)?)
        return self

    def _ensure_doc(self) -> "Doc":
        doc = self.doc_ref()
        if doc is None:
            raise ValueError(Errors.E866)
        return doc
