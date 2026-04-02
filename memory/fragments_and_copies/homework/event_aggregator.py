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

from datetime import datetime
from typing import Any

from memory.fragments_and_copies.homework import fake_boto3
from memory.fragments_and_copies.homework.memory_profiler import memory_profile


class EventAggregator:  # <- Залиш назву незмінною
    def __init__(self, bucket: str, prefix: str):  # <- Init метод має залишитися таким
        self.bucket = bucket
        self.prefix = prefix

        self.s3 = fake_boto3.client('s3')

    @memory_profile
    def run(self) -> list[dict]:  # <- метод run має бути наявним у класі, вміст можна змінювати за потреби
        payload: list[Any] = []
        event_by_user: dict[str, list[dict]] = {}

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
                user_id = event['user_id']

                normalized_event = {
                    'user_id': user_id,
                    'event': event['event'],
                    'value': event.get('value'),
                    'timestamp': datetime.fromisoformat(event['timestamp']).timestamp(),
                    'extra': {},
                }

                if user_id not in event_by_user:
                    event_by_user[user_id] = [normalized_event]
                else:
                    event_by_user[user_id].append(normalized_event)

        for user, events in event_by_user.items():
            payload.append({'user': user, 'count': len(events), 'events': events})

        return payload
