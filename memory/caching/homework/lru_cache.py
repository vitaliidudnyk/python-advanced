"""
Завдання:
    Реалізувати структуру даних LRU Cache (Least Recently Used), яка
    підтримує операції get() і put() за O(1) часу.

    LRU Cache має фіксовану місткість. Коли кеш переповнюється,
    необхідно видалити ключ, який використовувався
    найдавніше (least recently used - LRU).

Вимоги:
    - Реалізувати клас LRUCache з методами:
        - get(key) -> повертає значення або -1, якщо ключа немає.
        - put(key, value) -> додає або оновлює значення.
    - Часова складність обох операцій має бути O(1).
    - При додаванні нового елемента:
        - якщо є вільне місце - просто додати;
        - якщо кеш повний - видалити найменш використаний ключ.
    - Будь-яке звернення до ключа (get або put для ключа, що існує)
      робить цей ключ останнім використаним.

Технічні обмеження:
    - Заборонено використовувати готові OrderedDict або LRU-реалізації.
    - Дозволено використовувати:
        - словник для O(1) доступу до вузлів;
        - власну реалізацію двозв’язного списку для порядку LRU.
    - Заборонено створювати зайві копії даних.

Актуальність:
    LRU Cache є однією з найважливіших структур у сучасних високонавантажених
    системах. Вона використовується в:
        - кешах веб-серверів та API-гейтвеїв;
        - внутрішніх кешах Python-інтерпретатора (наприклад, bytecode cache);
        - СУБД та файлових системах;
        - Redis, Memcached, CDN-сервісах;
        - оптимізації ML-моделей, коли необхідно кешувати попередні результати.

    Розуміння механіки LRU - це базова навичка для backend/infra/ML-інженера,
    оскільки він поєднує роботу зі структурами даних, керування пам’яттю,
    оптимізацію доступу та алгоритмічне мислення.
"""


class Node:
    def __init__(self, key: int, value: int):
        self.key = key
        self.value = value
        self.prev: Node | None = None
        self.next: Node | None = None


class LRUCache:
    def __init__(self, capacity: int):
        self._cache: dict[int, Node] = {}
        self._capacity = capacity
        self._head: Node | None = None
        self._tail: Node | None = None

    def get(self, key: int) -> int:
        if key in self._cache:
            node = self._cache[key]
            self._move_to_head(node)
            return self._cache[key].value
        else:
            return -1

    def put(self, key: int, value: int) -> None:
        if key in self._cache:
            node = self._cache[key]
            self._move_to_head(node)
            self._cache[key].value = value
        else:
            node = Node(key, value)
            self._add_to_head(node, value)
            if len(self._cache) > self._capacity:
                self._remove_tail()

    def _remove_tail(self) -> None:
        if self._tail is None:
            return

        self._cache.pop(self._tail.key)
        if self._head is self._tail:
            self._head = None
            self._tail = None
            return

        self._tail = self._tail.prev

        if self._tail is not None:
            self._tail.next = None

    def _add_to_head(self, node: Node, value: int) -> None:
        self._cache[node.key] = node

        if self._head is None:
            self._head = self._tail = node
        else:
            self._head.prev = node
            node.next = self._head
            self._head = node

    def _move_to_head(self, node: Node) -> None:
        if node is self._head:
            return

        if node is self._tail:
            self._tail = node.prev
            if self._tail is not None:
                self._tail.next = None
        else:
            prev_node = node.prev
            if prev_node is not None:
                prev_node.next = node.next
            next_node = node.next
            if next_node is not None:
                next_node.prev = node.prev

        if self._head is not None:
            self._head.prev = node
        node.next = self._head
        node.prev = None
        self._head = node
