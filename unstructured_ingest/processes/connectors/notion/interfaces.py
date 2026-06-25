from abc import ABC, abstractmethod
from dataclasses import fields as _dataclass_fields
from typing import Optional

from htmlBuilder.tags import HtmlTag


def init_from_dict(cls, data: dict, **overrides):
    """Instantiate dataclass ``cls`` from ``data``, ignoring unknown keys.

    Notion regularly adds presentational/metadata keys to object payloads
    (for example ordered-list ``list_format`` on ``numbered_list_item``).
    Constructing with a bare ``cls(**data)`` raises ``TypeError`` the moment one
    of those additive, backwards-compatible keys appears. Filtering ``data`` down
    to the declared fields of ``cls`` keeps parsing resilient to such changes.

    Any ``overrides`` are applied last and always win, so callers can pass
    already-parsed nested values (e.g. ``rich_text=[...]``) without those keys
    being duplicated from ``data``.
    """
    allowed = {f.name for f in _dataclass_fields(cls)}
    kwargs = {k: v for k, v in data.items() if k in allowed and k not in overrides}
    kwargs.update(overrides)
    return cls(**kwargs)


class FromJSONMixin(ABC):
    @classmethod
    @abstractmethod
    def from_dict(cls, data: dict):
        pass


class GetHTMLMixin(ABC):
    @abstractmethod
    def get_html(self) -> Optional[HtmlTag]:
        pass


class BlockBase(FromJSONMixin, GetHTMLMixin):
    @staticmethod
    @abstractmethod
    def can_have_children() -> bool:
        pass


class DBPropertyBase(FromJSONMixin):
    pass


class DBCellBase(FromJSONMixin, GetHTMLMixin):
    pass
