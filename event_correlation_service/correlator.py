from dataclasses import dataclass

import dataclasses
from typing import Any, Iterable, Iterator, List, Optional, Set, Tuple, Union
import datetime
import copy

DEFAULT_TIME_KEYS = {'time', 'timestamp'}


@dataclass()
class EventCorrelator:
    input_streams: Iterable[Iterator[dict]]
    time_keys: Optional[Set[str]] = dataclasses.field(default_factory=copy.copy(DEFAULT_TIME_KEYS))
    window_size: Optional[Union[int, float]] = None

    def __post_init__(self):
        self.time_keys = set(self.time_keys or DEFAULT_TIME_KEYS)

    def run(self) -> Iterator[Tuple[dict, dict]]:
        while True:
            pass

    def __iter__(self) -> Iterator[Tuple[dict, dict]]:
        yield from self.run()


def iter_matching_events(stream_a: Iterable[dict], stream_b: Iterable[dict], time_keys: Optional[Iterable[str]] = None, event_ttl: Optional[Union[int, float]] = None, field_aliases: Optional[dict] = None, join_on: Optional[Iterable[str]] = None) -> Iterator[Tuple[dict, dict]]:
    stream_a = tuple(stream_a)
    stream_b = tuple(stream_b)
    for a in stream_a:
        for b in stream_b:
            matches = events_match(
                a=a, 
                b=b, 
                time_keys=time_keys,
                event_ttl=event_ttl,
                join_on=join_on, 
                field_aliases=field_aliases,
            )
            if matches:
                yield (a, b)


def events_match(
        a: dict, 
        b: dict, 
        time_keys: Optional[Iterable[str]] = None, 
        event_ttl: Optional[Union[int, float]] = None, 
        field_aliases: Optional[dict] = None, 
        join_on: Optional[Iterable[str]] = None) -> bool:

    a = copy.copy(a)
    b = copy.copy(b)

    a = flatten_dict(a)
    b = flatten_dict(b)

    if field_aliases:
        a = rename_fields(a, field_aliases=field_aliases)
        b = rename_fields(b, field_aliases=field_aliases)
    
    if event_ttl and event_ttl > 0:
        if not time_keys:
            raise ValueError('time_keys must be specified when event_ttl > 0')

        a_times = parse_timestamps([a[k] for k in time_keys if k in a])
        b_times = parse_timestamps([b[k] for k in time_keys if k in b])
        if not (a_times and b_times):
            return False
        elif len(a_times) > 1 or len(b_times) > 1:
            raise NotImplementedError('Events with multiple times are not supported')
        else:
            a_time = a_times[0]
            b_time = b_times[0]
            if a_time > b_time:
                d = (a_time - b_time).total_seconds()
            elif a_time < b_time:
                d = (b_time - a_time).total_seconds()
            else:
                d = 0
            
            if d > event_ttl:
                return False

    if join_on:
        a = {k: v for k, v in a.items() if k in join_on}
        b = {k: v for k, v in b.items() if k in join_on}
    
    return a == b


def rename_fields(data: dict, field_aliases: dict) -> dict:
    for (new_name, old_names) in field_aliases.items():
        for old_name in old_names:
            if old_name in data:
                data[new_name] = data.pop(old_name)
    return data


def parse_timestamps(timestamps: Iterable[Any], sort_ascending: bool = True) -> List[datetime.datetime]:
    timestamps = list(map(parse_timestamp, timestamps))
    if sort_ascending:
        timestamps = sorted(timestamps)
    return timestamps


def parse_timestamp(timestamp: Any) -> datetime.datetime:
    if isinstance(timestamp, (int, float)):
        return datetime.datetime.fromtimestamp(timestamp)
    elif isinstance(timestamp, str):
        return datetime.datetime.fromisoformat(timestamp)
    elif isinstance(timestamp, datetime.datetime):
        return timestamp
    else:
        raise ValueError(f'Invalid timestamp type: {type(timestamp)}')


def flatten_dict(d):
    def items() -> Iterator[Tuple[str, Any]]:
        for k, v in d.items():
            if isinstance(v, dict):
                for _k, _v in flatten_dict(v).items():
                    yield k + "." + _k, _v
            else:
                yield k, v
    return dict(items())
