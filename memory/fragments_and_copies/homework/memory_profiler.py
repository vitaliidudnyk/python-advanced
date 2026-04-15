"""
Реалізувати комплексний декоратор для профілювання пам'яті в Python.

Завдання:
    Створити декоратор @memory_profile, який вимірює ключові метрики використання памʼяті.
    Формат вільний. Зробити так, як буде зручно саме тобі, щоб використовувати його для аналізу коду.
    Ідеї, що може виконувати декоратор:
        - Знімати snapshots памʼяті ДО та ПІСЛЯ виконання функції.
        - Фіксувати:
            - сумарні алокації між snapshots;
            - поточне та пікове використання памʼяті;
            - час виконання функції;
            - RSS процесу до/після.
        - Показувати топ-рядків коду, що створили найбільше алокацій.
        - Визначати "hotspots" - ймовірні місця створення копій (за кількістю та розміром алокацій).

Актуальність:
    Глибоке профілювання пам'яті - критична навичка для оптимізації Python коду,
    де великі структури даних, копії та зайві алокації можуть створювати
    приховані ботлнеки. Це дозволяє зрозуміти, де саме виникають копії,
    яка пікова памʼять потрібна коду, і як поводиться програма під навантаженням.
    Це фундаментальна техніка для систем високої пропускної здатності,
    асинхронних сервісів та задач інжинірингу даних.
"""

import functools
import os
import time
import tracemalloc

from typing import Any, Callable

import psutil


def memory_profile(func: Callable) -> Callable:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        process = psutil.Process(os.getpid())

        rss_before = process.memory_info().rss

        tracemalloc.start()
        snapshot_before = tracemalloc.take_snapshot()
        start_time = time.perf_counter()

        try:
            result = func(*args, **kwargs)
            return result
        finally:
            current, peak = tracemalloc.get_traced_memory()
            elapsed = time.perf_counter() - start_time
            snapshot_after = tracemalloc.take_snapshot()
            rss_after = process.memory_info().rss

            stats = snapshot_after.compare_to(snapshot_before, 'lineno')
            filtered_stats = [stat for stat in stats if _is_interesting_stat(stat)]

            total_positive_diff = sum(stat.size_diff for stat in stats if stat.size_diff > 0)
            total_negative_diff = sum(stat.size_diff for stat in stats if stat.size_diff < 0)
            net_diff = sum(stat.size_diff for stat in stats)

            tracemalloc.stop()

            print(f'Function: {func.__name__}')
            print(f'Elapsed time: {elapsed:.6f} seconds')
            print(f'RSS before: {rss_before / 1024:.2f} KiB')
            print(f'RSS after: {rss_after / 1024:.2f} KiB')
            print(f'RSS delta: {(rss_after - rss_before) / 1024:.2f} KiB')
            print(f'Current trace memory usage: {current / 1024:.2f} KiB')
            print(f'Peak memory usage: {peak / 1024:.2f} KiB')
            print(f'Total positive diff: {_format_size(total_positive_diff, sign=True)}')
            print(f'Total negative diff: {_format_size(total_negative_diff, sign=True)}')
            print(f'Net diff: {_format_size(net_diff, sign=True)}')

            print('\nTop allocated diffs:')
            for index, stat in enumerate(filtered_stats[:5], start=1):
                frame = stat.traceback[0]
                average_size = 0.0

                if stat.count_diff != 0:
                    average_size = stat.size_diff / stat.count_diff

                print(
                    f'{index}. {frame.filename}: {frame.lineno} | '
                    f'size diff={_format_size(stat.size_diff, sign=False)} | '
                    ', '
                    f'count diff={stat.count_diff}, '
                    f'avg={average_size:.2f} B'
                )

    return wrapper


def _is_interesting_stat(stat: tracemalloc.StatisticDiff) -> bool:
    if stat.size_diff <= 0:
        return False

    frame = stat.traceback[0]
    filename = frame.filename

    ignore_parts = (
        'tracemalloc',
        'psutil',
        'importlib',
        'site-packages',
    )

    return not any(part in filename for part in ignore_parts)


def _format_size(size_bytes: int, sign: bool = False) -> str:
    value = size_bytes / 1024

    if sign:
        return f'{value:+.2f} KiB'

    return f'{value:.2f} KiB'
