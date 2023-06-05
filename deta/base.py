import datetime
import os
from typing import Any, Dict, List, Mapping, Optional, Sequence, Union, overload
from urllib.parse import quote

from .service import JSON_MIME, _Service

# timeout for Base service in seconds
BASE_SERVICE_TIMEOUT = 300
BASE_TTL_ATTRIBUTE = "__expires"

# supported DetaBase types
Primitive = Union[str, int, float, bool, None]
Array = List[Any]
Object = Dict[str, Union[Any, "Object", Array]]
# Data supported by DetaBase
Data = Union[Primitive, Array, Object]
# Item is object with key attribute
Item = Object
# Deta Base query. Support hierarchical queries with nested dicts.
Query = Mapping[str, Union[Data, "Query"]]
# Update is a mapping of attribute to update operation. Supports hierarchical updates with nested dicts.
Update = Mapping[str, Union[Data, "Update"]]
# Updates is a mapping of key to Update.
Updates = Mapping[str, Union[Data, "Update"]]


class FetchResponse:
    def __init__(
        self, count: int = 0, last: Optional[str] = None, items: Optional[List[Item]] = None
    ):
        self.count = count
        self.last = last
        self.items = items if items is not None else []

    def __eq__(self, other: "FetchResponse"):
        return self.count == other.count and self.last == other.last and self.items == other.items

    def __iter__(self):
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)


class Util:
    class Trim:
        pass

    class Increment:
        def __init__(self, value: Union[int, float] = 1):
            self.value = value

    class Append:
        def __init__(self, value: Data):
            self.value = value if isinstance(value, list) else [value]

    class Prepend:
        def __init__(self, value: Data):
            self.value = value if isinstance(value, list) else [value]

    def trim(self):
        return self.Trim()

    def increment(self, value: Union[int, float] = 1):
        return self.Increment(value)

    def append(self, value: Data):
        return self.Append(value)

    def prepend(self, value: Data):
        return self.Prepend(value)


class _Base(_Service):
    def __init__(self, name: str, project_key: str, project_id: str, *, host: Optional[str] = None):
        if not name:
            raise ValueError("parameter 'name' must be a non-empty string")

        host = host or os.getenv("DETA_BASE_HOST") or "database.deta.sh"
        super().__init__(project_key, project_id, host, name, BASE_SERVICE_TIMEOUT)
        self._ttl_attribute = BASE_TTL_ATTRIBUTE
        self.util = Util()

    def get(self, key: str) -> Item:
        if not key:
            raise ValueError("parameter 'key' must be a non-empty string")

        key = quote(key, safe="")
        _, res = self._request(f"/items/{key}", "GET")
        return res

    def delete(self, key: str) -> None:
        """Delete an item from the database

        Args:
            key: The key of item to be deleted.
        """
        if not key:
            raise ValueError("parameter 'key' must be a non-empty string")

        key = quote(key, safe="")
        self._request(f"/items/{key}", "DELETE")

    @overload
    def insert(
        self,
        data: Data,
        key: Optional[str] = None,
    ) -> Item:
        ...

    @overload
    def insert(
        self,
        data: Data,
        key: Optional[str] = None,
        *,
        expire_in: int,
    ) -> Item:
        ...

    @overload
    def insert(
        self,
        data: Data,
        key: Optional[str] = None,
        *,
        expire_at: Union[int, float, datetime.datetime],
    ) -> Item:
        ...

    def insert(self, data, key=None, *, expire_in=None, expire_at=None) -> Item:
        data = data.copy() if isinstance(data, dict) else {"value": data}
        if key:
            data["key"] = key

        insert_ttl(data, self._ttl_attribute, expire_in, expire_at)
        code, res = self._request("/items", "POST", {"item": data}, content_type=JSON_MIME)

        if code == 201:
            return res
        elif code == 409:
            raise ValueError(f"item with  key '{key}' already exists")

    @overload
    def put(
        self,
        data: Data,
        key: Optional[str] = None,
    ) -> Item:
        ...

    @overload
    def put(
        self,
        data: Data,
        key: Optional[str] = None,
        *,
        expire_in: int,
    ) -> Item:
        ...

    @overload
    def put(
        self,
        data: Data,
        key: Optional[str] = None,
        *,
        expire_at: Union[int, float, datetime.datetime],
    ) -> Item:
        ...

    def put(self, data, key=None, *, expire_in=None, expire_at=None) -> Item:
        """Store (put) an item in the database. Overrides an item if key already exists.
        `key` could be provided as an argument or a field in the data dict.
        If `key` is not provided, the server will generate a random 12-character key.
        """
        data = data.copy() if isinstance(data, dict) else {"value": data}
        if key:
            data["key"] = key

        insert_ttl(data, self._ttl_attribute, expire_in, expire_at)
        code, res = self._request("/items", "PUT", {"items": [data]}, content_type=JSON_MIME)
        return res["processed"]["items"][0] if res and code == 207 else None

    @overload
    def put_many(
        self,
        items: Sequence[Data],
    ) -> Dict[str, Dict[str, List[Item]]]:
        ...

    @overload
    def put_many(
        self,
        items: Sequence[Data],
        *,
        expire_in: int,
    ) -> Dict[str, Dict[str, List[Item]]]:
        ...

    @overload
    def put_many(
        self,
        items: Sequence[Data],
        *,
        expire_at: Union[int, float, datetime.datetime],
    ) -> Dict[str, Dict[str, List[Item]]]:
        ...

    def put_many(
        self, items, *, expire_in=None, expire_at=None
    ) -> Dict[str, Dict[str, List[Item]]]:
        if len(items) > 25:
            raise ValueError("cannot put more than 25 items at a time")

        _items = []
        for item in items:
            data = item
            if not isinstance(item, dict):
                data = {"value": item}
            insert_ttl(data, self._ttl_attribute, expire_in, expire_at)
            _items.append(data)

        _, res = self._request("/items", "PUT", {"items": _items}, content_type=JSON_MIME)
        return res

    def fetch(
        self,
        query: Optional[Query] = None,
        *,
        limit: int = 1000,
        last: Optional[str] = None,
    ) -> FetchResponse:
        """Fetch items from the database. `query` is an optional filter or list of filters.
        Without a filter, it will return the whole db.
        """
        payload = {
            "limit": limit,
            "last": last if not isinstance(last, bool) else None,
        }

        if query:
            payload["query"] = query if isinstance(query, Sequence) else [query]

        _, res = self._request("/query", "POST", payload, content_type=JSON_MIME)
        paging = res.get("paging")
        return FetchResponse(paging.get("size"), paging.get("last"), res.get("items"))

    @overload
    def update(
        self,
        updates: Updates,
        key: str,
    ) -> None:
        ...

    @overload
    def update(
        self,
        updates: Updates,
        key: str,
        *,
        expire_in: int,
    ) -> None:
        ...

    @overload
    def update(
        self,
        updates: Updates,
        key: str,
        *,
        expire_at: Union[int, float, datetime.datetime],
    ) -> None:
        ...

    def update(self, updates: Updates, key, *, expire_in=None, expire_at=None) -> None:
        """Update an item in the database.
        `updates` specifies the attribute names and values to update, add or remove.
        `key` is the key of the item to be updated.
        """
        if not key:
            raise ValueError("parameter 'key' must be a non-empty string")

        payload = {
            "set": {},
            "increment": {},
            "append": {},
            "prepend": {},
            "delete": [],
        }

        if updates:
            for attr, value in updates.items():
                if isinstance(value, Util.Trim):
                    payload["delete"].append(attr)
                elif isinstance(value, Util.Increment):
                    payload["increment"][attr] = value.value
                elif isinstance(value, Util.Append):
                    payload["append"][attr] = value.value
                elif isinstance(value, Util.Prepend):
                    payload["prepend"][attr] = value.value
                else:
                    payload["set"][attr] = value

        insert_ttl(payload["set"], self._ttl_attribute, expire_in, expire_at)

        encoded_key = quote(key, safe="")
        code, _ = self._request(f"/items/{encoded_key}", "PATCH", payload, content_type=JSON_MIME)
        if code == 404:
            raise ValueError(f"key '{key}' not found")


def insert_ttl(
    item: Item,
    ttl_attribute: str,
    expire_in: Optional[Union[int, float]] = None,
    expire_at: Optional[Union[int, float, datetime.datetime]] = None,
):
    if expire_in and expire_at:
        raise ValueError("'expire_in' and 'expire_at' are mutually exclusive parameters")

    if not expire_in and not expire_at:
        return

    if expire_in:
        expire_at = datetime.datetime.now() + datetime.timedelta(seconds=expire_in)

    if isinstance(expire_at, datetime.datetime):
        expire_at = expire_at.replace(microsecond=0).timestamp()
    elif not isinstance(expire_at, (int, float)):
        raise TypeError("'expire_at' must be of type 'int', 'float' or 'datetime'")

    item[ttl_attribute] = int(expire_at)
