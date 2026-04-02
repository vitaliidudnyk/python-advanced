"""
Реалізувати Copy-On-Write масив.

Copy-On-Write (COW) - структура, де кілька масивів можуть ділити один
спільний буфер даних. Копія створюється лише під час першої операції
запису у конкретний масив; читання не робить копій.

Вимоги:
    - Використати внутрішній @dataclass Storage(data, refcount);
    - Масиви, створені через cow_copy(), ділять один буфер;
    - Будь-який запис (set, delete, insert, append) має:
        - перевіряти refcount;
        - від’єднувати буфер (copy), якщо refcount > 1.
    - Не створювати копій при cow_copy() або читанні.

Актуальність:
    Програміст стикається з Copy-On-Write у задачах, де потрібно працювати
    з великими масивами даних без зайвих копій. Це підхід, який лежить в основі:
        - snapshot-структур (історія змін, undo/redo);
        - кешів та memoization, де потрібні lightweight-копії стану;
        - паралельних обчислень, коли кілька процесів читають спільні дані;
        - zero-copy операцій у NumPy, PyArrow, Pandas;
        - обробки файлів та буферів без дублювання памʼяті;
        - fork-процесів у Python, де сторінки памʼяті COW до першого запису.

    Вміння реалізувати COW у Python допомагає оптимізувати памʼять,
    будувати ефективні структури даних і розуміти, як працюють сучасні high-performance бібліотеки.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable


@dataclass
class Storage:
    """
    Внутрішній буфер для Copy-On-Write масиву.

    Поля:
        data     - фактичний список елементів.
        refcount - кількість масивів, що спільно використовують цей буфер.
    """

    data: list[Any]
    refcount: int = 1


class CopyOnWriteArray:
    """
    Copy-On-Write масив.

    Працює як звичайний список, але копіює дані лише тоді,
    коли відбувається запис у масив, що ділить буфер з іншими екземплярами.
    Корисно для оптимального використання памʼяті та легких "знімків" стану.
    """

    def __init__(self, items: Iterable[Any] | None = None) -> None:
        """
        Ініціалізує масив із власним буфером.
        Створює окремий Storage й не ділить його з іншими масивами (назви його _storage).
        """
        if items is None:
            data: list[Any] = []
        elif isinstance(items, list):
            data = items
        else:
            data = list(items)

        self._storage = Storage(data, 1)

    def cow_copy(self) -> CopyOnWriteArray:
        """
        Повертає легку копію масиву, що розділяє той самий буфер.
        Копіювання даних не відбувається.
        """
        new_obj = CopyOnWriteArray.__new__(CopyOnWriteArray)
        new_obj._storage = self._storage
        self._storage.refcount += 1
        return new_obj

    def __len__(self) -> int:
        return len(self._storage.data)

    def __getitem__(self, index: int) -> Any:
        """
        Повертає елемент за індексом.
        Завжди читає напряму зі спільного буфера.
        """
        return self._storage.data[index]

    def __setitem__(self, index: int, value: Any) -> None:
        """
        Записує значення за індексом.
        Якщо буфер спільний - створює власну копію перед записом.
        """
        self._ensure_own_copy()

        self._storage.data[index] = value

    def __delitem__(self, index: int) -> None:
        """
        Видаляє елемент.
        Перед операцією може знадобитися відʼєднати власний буфер.
        """
        self._ensure_own_copy()

        del self._storage.data[index]

    def insert(self, index: int, value: Any) -> None:
        """
        Вставляє новий елемент за індексом.
        При спільному буфері створює власну копію перед модифікацією.
        """
        self._ensure_own_copy()

        self._storage.data.insert(index, value)

    def append(self, value: Any) -> None:
        """
        Додає елемент у кінець масиву.
        Викликає відʼєднання буфера за потреби.
        """
        self._ensure_own_copy()

        self._storage.data.append(value)

    def to_list(self) -> list[Any]:
        """
        Повертає новий звичайний Python-список.
        Це завжди реальна копія даних.
        """
        return list(self._storage.data)

    def __repr__(self) -> str:
        """
        Повертає зручне текстове представлення масиву для дебагу.
        Показує дані й стан буфера.
        """
        return f'refcount={self._storage.refcount}, data={self._storage.data}'

    def _ensure_own_copy(self):
        if self._storage.refcount > 1:
            new_storage = Storage(self._storage.data.copy(), 1)
            self._storage.refcount -= 1
            self._storage = new_storage
