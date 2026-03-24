"""
Завдання:
    Реалізувати аналіз великого лог-файлу та знайти максимальну суму значень duration у ковзному вікні з K рядків.

    Формат рядка:
        timestamp;service;duration

    Приклад:
        1719921001;auth;34

    Необхідно:
        - прочитати лог-файл;
        - витягнути значення duration;
        - знайти максимальну суму у ковзному вікні розміру K.

Вимоги:
    - файл може містити десятки мільйонів рядків;
    - рішення має працювати потоково (streaming);
    - заборонено завантажувати весь файл у памʼять;
    - необхідно мінімізувати кількість алокацій;
    - не створювати зайві списки або копії рядків;

Технічні обмеження:
    - дозволено використовувати стандартну бібліотеку Python;
    - заборонено pandas / numpy;
    - заборонено читати файл повністю в памʼять;
    - бажано уникати зайвих split() та list().

Актуальність:
    Аналіз великих логів є типовою задачею у системах observability, monitoring та distributed systems.

    У production-системах такі задачі часто обробляють терабайти даних, тому оптимальне використання памʼяті
    та мінімізація алокацій є критично важливими.

    Це завдання тренує:
        - streaming обробку даних;
        - оптимізацію алокацій;
        - ефективні структури даних;
        - роботу з ковзними вікнами;
        - memory-efficient Python код.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Deque, TextIO

from memory.memory_anatomy.homework.log_window_analyzer.generator import LogFileGenerator


@dataclass(slots=True, frozen=True)
class WindowResult:
    """
    Результат аналізу лог-файлу.
    """

    max_window_sum: int
    window_size: int
    processed_rows: int


class LogWindowAnalyzer:
    """
    Аналізатор великого лог-файлу.
    Необхідно знайти максимальну суму значень duration у ковзному вікні розміру K.
    """

    def __init__(self, window_size: int):
        if window_size <= 0:
            raise ValueError('window_size must be greater than 0')

        self._window_size = window_size

    def process_file(self, path: Path) -> WindowResult:
        """
        Відкрити файл та передати файловий потік у метод _process_stream.
        """
        with path.open('r', encoding='utf-8') as stream:
            return self._process_stream(stream)

    def _process_stream(self, stream: TextIO) -> WindowResult:
        """
        Потоково обробити лог-файл.

        Потрібно:
            - читати файл рядок за рядком;
            - витягувати duration через _parse_duration();
            - підтримувати ковзне вікно розміру self._window_size;
            - знайти максимальну суму duration у цьому вікні.

        Важливо:
            - не можна читати файл повністю в памʼять;
            - не створювати зайві списки;
            - мінімізувати кількість алокацій.
        """

        max_window_sum = 0
        current_window_sum = 0
        rows = 0
        q: Deque[int] = deque(maxlen=self._window_size)
        for line in stream:
            if line == '\n':
                continue

            rows += 1
            duration_int = self._parse_duration(line)

            if len(q) >= self._window_size:
                prev = q.popleft()
                current_window_sum -= prev

            q.append(duration_int)
            current_window_sum += duration_int

            if current_window_sum > max_window_sum:
                max_window_sum = current_window_sum

        return WindowResult(max_window_sum, self._window_size, rows)

    @staticmethod
    def _parse_duration(line: str) -> int:
        """
        Витягнути значення duration з рядка формату:
            timestamp;service;duration

        Заборонено:
            - використовувати split(), щоб уникнути зайвих алокацій.
        """
        if line.count(';') != 2:
            raise ValueError('invalid line')

        start = line.rfind(';') + 1
        end = len(line) - 1 if line.endswith('\n') else len(line)
        duration_str = line[start:end]
        if not duration_str.isdigit():
            raise ValueError('invalid duration')

        return int(duration_str)


def main() -> None:
    generator = LogFileGenerator(
        services=('auth', 'payments', 'search', 'profile'),
        duration_min=5,
        duration_max=300,
    )

    generator.generate(
        path=Path('data/test_logs.txt'),
        rows=5_000_000,
    )

    analyzer = LogWindowAnalyzer(window_size=3)

    result = analyzer.process_file(Path('data/test_logs.txt'))

    print(f'Processed rows: {result.processed_rows}')
    print(f'Window size: {result.window_size}')
    print(f'Max window sum: {result.max_window_sum}')


if __name__ == '__main__':
    main()
