from typing import Iterable, Tuple, Union, Optional, TYPE_CHECKING
import warnings
import weakref
from collections import defaultdict, UserDict
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
        bytes_keys = defaultdict(list)
        for key, value in self.items():
            bytes_keys[value.to_bytes()].append(key)
        return srsly.msgpack_dumps(bytes_keys)

    def from_bytes(self, bytes_data: bytes) -> "SpanGroups":
        msg = (
            []
            if not bytes_data or bytes_data == self._EMPTY_BYTES
            else srsly.msgpack_loads(bytes_data)
        )
        self.clear()
        doc = self._ensure_doc()
        # backwards-compatibility: bytes_data may be one of:
        # b'', a serialized empty list, a serialized list of SpanGroup bytes,
        # (and now it may be) a serialized dict mapping keys to SpanGroup bytes
        if isinstance(msg, list):
            # This is either the ~1st version of `SpanGroups` serialization
            # or there were no SpanGroups serialized
            for value_bytes in msg:
                group = SpanGroup(doc).from_bytes(value_bytes)
                if group.name in self:
                    # Display a warning if `msg` contains `SpanGroup`s
                    # that have the same .name (attribute).
                    # Because, for `SpanGroups` serialized as lists,
                    # only 1 SpanGroup per .name is loaded. (See #10685)
                    warnings.warn(
                        Warnings.W119.format(group_name=group.name, group_values=group)
                    )
                    continue
                self[group.name] = group
        else:
            # The ~2nd version of `SpanGroups` serialization--a dict
            for value_bytes, keys in msg.items():
                group = SpanGroup(doc).from_bytes(value_bytes)
                # Set the first key to the SpanGroup just created.
                # Set any remaining keys to copies of that SpanGroup,
                # because we can't assume they were all the same identical object:
                # it's possible that 2 different SpanGroup objects (pre-serialization)
                # had the same bytes, and mapped to the same `msg` key.
                self[keys[0]] = group
                for key in keys[1:]:
                    self[key] = group.copy()
        return self

    def _ensure_doc(self) -> "Doc":
        doc = self.doc_ref()
        if doc is None:
            raise ValueError(Errors.E866)
        return doc
