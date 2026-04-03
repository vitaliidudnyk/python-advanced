"""
Оптимізувати клас EventAggregator для мінімізації копій памʼяті.

Завдання:
    Поточна реалізація EventAggregator створює багато непотрібних копій. Потрібно
    переписати логіку так, щоб обробка подій була максимально ефективною за памʼяттю.

Вимоги:
    - Назва класу EventAggregator та метод __init__ мають залишитися незмінними;
    - Метод run() має бути присутнім; його вміст можна змінювати за потреби;
    - Більше обмежень по модифікаціям у класі немає;
    - Мета - мінімізувати алокації, копії в памʼяті;
    - Фінальний результат має формувати payload по користувачах у тому ж форматі.

Актуальність:
    Робота з великими JSON-потоками та S3 - типова задача в системах обробки подій.
    Надмірні копії призводять до високого споживання памʼяті та CPU.
    Оптимізація дозволяє побудувати ефективний pipeline і продемонструвати
    практичне розуміння методів мінімізації алокацій та роботи зі структурами даних у Python.
"""

import json

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterator

from memory.fragments_and_copies.homework import fake_boto3
from memory.fragments_and_copies.homework.memory_profiler import memory_profile


@dataclass(slots=True)
class NormalizedEven:
    user_id: str
    event: str
    value: float | None
    timestamp: float
    extra: dict[str, Any] | None


@dataclass(slots=True)
class PayloadUserEvents:
    user: str
    count: int
    events: list[NormalizedEven]


class EventAggregator:  # <- Залиш назву незмінною
    def __init__(self, bucket: str, prefix: str):  # <- Init метод має залишитися таким
        self.bucket = bucket
        self.prefix = prefix

        self.s3 = fake_boto3.client('s3')

    @memory_profile
    def run(self) -> list[PayloadUserEvents]:  # <- метод run має бути наявним у класі, вміст можна змінювати за потреби
        event_by_user: dict[str, list[NormalizedEven]] = {}
        for norm_event in self._iter_normalized_even():
            if norm_event.user_id not in event_by_user:
                event_by_user[norm_event.user_id] = [norm_event]
            else:
                event_by_user[norm_event.user_id].append(norm_event)

        return list(self._iter_payload(event_by_user))

    def _iter_raw_events(self) -> Iterator[dict]:
        objects = self.s3.list_objects_v2(
            Bucket=self.bucket,
            Prefix=self.prefix,
        )['Contents']

        for obj in objects:
            raw = self.s3.get_object(
                Bucket=self.bucket,
                Key=obj['Key'],
            )['Body'].read()

            for event in json.loads(raw):
                yield event

    def _iter_normalized_even(self) -> Iterator[NormalizedEven]:
        for event in self._iter_raw_events():
            yield NormalizedEven(
                event['user_id'],
                event['event'],
                event.get('value'),
                datetime.fromisoformat(event['timestamp']).timestamp(),
                None,
            )

    @staticmethod
    def _iter_payload(users: dict[str, list[NormalizedEven]]) -> Iterator[PayloadUserEvents]:
        for user, events in users.items():
            yield PayloadUserEvents(
                user,
                len(events),
                events,
            )
