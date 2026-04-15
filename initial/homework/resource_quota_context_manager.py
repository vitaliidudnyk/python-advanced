"""
Завдання: Контекстний менеджер з обмеженням ресурсу

Мета:
    Освіжити знання про:
    - протокол контекстного менеджера (__enter__ / __exit__);
    - контроль та облік ресурсу;
    - збереження стану обʼєкта;
    - гарантоване звільнення ресурсу.

Контекст:
    У реальних системах ресурси обмежені памʼяттю, зʼєднанням, файловими дескрипторами, API quota.
    Контекстний менеджер дозволяє коректно резервувати ресурс і гарантовано повертати його назад.

Умова:
    Реалізуйте контекстний менеджер ResourceQuota, який:

    - має загальний ліміт ресурсу (total_limit);
    - при вході в контекст резервує requested одиниць ресурсу;
    - якщо ресурсу недостатньо - кидає ValueError;
    - при виході з контексту завжди повертає ресурс.

    Ресурс - абстрактний (просто число).

Вимоги:
    - реалізуйте __enter__ та __exit__;
    - не використовуйте глобальні змінні;
    - стан ресурсу має зберігатись у класі;
    - __exit__ повинен звільняти ресурс навіть при винятках.

Приклад:
    quota = ResourceQuota(total_limit=10)
    with quota.request(4):
        pass
"""

from types import TracebackType
from typing import Type


class ResourceQuota:
    def __init__(self, total_limit: int):
        self._total_limit = total_limit
        self.used = 0

    def request(self, amount: int) -> '_QuotaContext':
        return _QuotaContext(self, amount)

    def reserve(self, amount: int):
        if self.used + amount > self._total_limit:
            raise ValueError('Not enough resources')
        else:
            self.used += amount

    def release(self, amount: int):
        self.used -= amount


class _QuotaContext:
    def __init__(self, quota: ResourceQuota, amount: int):
        self._quota = quota
        self._amount = amount

    def __enter__(self) -> int:
        self._quota.reserve(self._amount)
        return self._amount

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ):
        self._quota.release(self._amount)
