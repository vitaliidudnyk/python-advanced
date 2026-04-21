"""
Реалізувати задачу "House Robber" трьома методами динамічного програмування:
    1) Рекурсія з memoization (Top-Down) - O(n) пам’яті;
    2) Табуляція (Bottom-Up) з масивом - O(n) пам’яті;
    3) Оптимізована версія (Bottom-Up) з O(1) пам’яттю - опціонально.

Завдання:
    Є список будинків, де кожен елемент - сума грошей у будинку.
    Злодій не може грабувати два сусідні будинки.
    Потрібно повернути максимальну суму, яку можна викрасти.

    Опціонально - намалювати діаграму роботи алгоритму для мемоізації та табуляції

Актуальність:
    Три підходи демонструють еволюцію алгоритмів динамічного програмування:
        - Memoization: простий для розуміння, але рекурсивний;
        - Табуляція: ітеративний, контрольований, без рекурсії;
        - Оптимізований варіант: максимально ефективний по пам’яті.
    Такі техніки широко використовуються у кешуванні, оптимізації
    підзадач, аналізі часової/просторової складності та реальних
    системах з обмеженнями по пам’яті.
"""

from typing import Iterable


class HouseRobberMemoized:
    """House Robber - рекурсія + мемоізація (Top-Down Dynamic Programming)."""

    def __init__(self):
        self.cache = None

    def rob(self, nums: Iterable[int]) -> int:
        self.cache = {}
        list_nums = list(nums)
        return self.best(list_nums, 0)

    def best(self, nums: list[int], idx: int) -> int:
        if idx >= len(nums):
            return 0

        if idx in self.cache:
            return self.cache[idx]

        max_val = max(nums[idx] + self.best(nums, idx + 2), self.best(nums, idx + 1))
        self.cache[idx] = max_val
        return max_val


class HouseRobberTabulated:
    """House Robber - табуляція (Bottom-Up Dynamic Programming)."""

    def rob(self, nums: Iterable[int]) -> int:
        dp: list[int] = [0] * 2
        idx = 2
        for num in nums:
            max_val = max(dp[idx - 1], dp[idx - 2] + num)
            dp.append(max_val)
            idx += 1

        return dp[idx - 1]


class HouseRobberOptimized:
    """House Robber - оптимізована табуляція з O(1) памʼяттю. Опціонально"""

    def rob(self, nums: Iterable[int]) -> int:
        max_sum1 = 0
        max_sum2 = 0
        for num in nums:
            max_sum3 = max(max_sum2, max_sum1 + num)
            max_sum1 = max_sum2
            max_sum2 = max_sum3

        return max(max_sum1, max_sum2)
