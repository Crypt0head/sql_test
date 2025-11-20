# Описание работы счетчика JOIN'ов

## Общая концепция

Счетчик JOIN'ов предназначен для сравнения двух методов подсчета:
1. **Подсчет через регулярное выражение** - простой поиск по тексту SQL
2. **Подсчет через sqlglot** - фактическая обработка JOIN'ов парсером

Это позволяет понять, какой процент JOIN'ов из исходных SQL-файлов успешно обрабатывается скриптом.

---

## 1. Счетчик через регулярное выражение

### Функция: `count_joins_by_regex()`

**Расположение:** строки 19-28 в `extract_joins.py`

**Принцип работы:**
```python
def count_joins_by_regex(sql_text: str) -> int:
    """
    Подсчитывает количество JOIN'ов в SQL тексте при помощи регулярного выражения.
    Ищет различные типы JOIN: INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL JOIN, просто JOIN.
    """
    # Паттерн для поиска JOIN'ов (case-insensitive)
    join_pattern = r'\b(?:INNER\s+)?JOIN\b|\bLEFT\s+(?:OUTER\s+)?JOIN\b|\bRIGHT\s+(?:OUTER\s+)?JOIN\b|\bFULL\s+(?:OUTER\s+)?JOIN\b'
    matches = re.findall(join_pattern, sql_text, re.IGNORECASE)
    return len(matches)
```

### Как работает регулярное выражение:

Паттерн `r'\b(?:INNER\s+)?JOIN\b|\bLEFT\s+(?:OUTER\s+)?JOIN\b|\bRIGHT\s+(?:OUTER\s+)?JOIN\b|\bFULL\s+(?:OUTER\s+)?JOIN\b'` ищет:

1. **`\b(?:INNER\s+)?JOIN\b`** - находит:
   - `JOIN` (без префикса, обычно означает INNER JOIN)
   - `INNER JOIN`
   - `inner join` (case-insensitive)

2. **`\bLEFT\s+(?:OUTER\s+)?JOIN\b`** - находит:
   - `LEFT JOIN`
   - `LEFT OUTER JOIN`
   - `left join` (case-insensitive)

3. **`\bRIGHT\s+(?:OUTER\s+)?JOIN\b`** - находит:
   - `RIGHT JOIN`
   - `RIGHT OUTER JOIN`
   - `right join` (case-insensitive)

4. **`\bFULL\s+(?:OUTER\s+)?JOIN\b`** - находит:
   - `FULL JOIN`
   - `FULL OUTER JOIN`
   - `full join` (case-insensitive)

### Особенности:

- **`\b`** - граница слова (word boundary), гарантирует, что мы не найдем JOIN внутри других слов
- **`(?:...)`** - non-capturing group, группировка без сохранения результата
- **`\s+`** - один или более пробельных символов
- **`re.IGNORECASE`** - поиск без учета регистра

### Примеры:

```sql
-- Найдет 3 JOIN'а:
SELECT * FROM table1
JOIN table2 ON ...
LEFT JOIN table3 ON ...
INNER JOIN table4 ON ...
```

### Когда вызывается:

- Вызывается **один раз** при инициализации объекта `JoinExtractor` (строка 58)
- Результат сохраняется в `self.regex_join_count`
- Это **базовая линия** (baseline) - сколько JOIN'ов есть в исходном тексте

---

## 2. Счетчик через sqlglot

### Переменная: `self.count`

**Расположение:** 
- Инициализация: строка 56 в `extract_joins.py`
- Увеличение счетчика: строка 247 в методе `_collect_joins()`

### Принцип работы:

```python
# В __init__:
self.count = 0  # Начальное значение

# В _collect_joins():
joins = select_expr.args.get("joins") or []
for join in joins:
    self.count += 1  # Увеличиваем счетчик для каждого найденного JOIN
    # ... дальнейшая обработка JOIN'а
```

### Как работает:

1. **Парсинг SQL:** sqlglot парсит SQL-текст и строит абстрактное синтаксическое дерево (AST)

2. **Поиск SELECT:** скрипт находит все SELECT-запросы в дереве:
   ```python
   all_selects: List[exp.Select] = []
   for tree in self._trees:
       all_selects.extend(tree.find_all(exp.Select))
   ```

3. **Обработка каждого SELECT:** для каждого SELECT извлекаются JOIN'ы:
   ```python
   for select in all_selects:
       # ...
       self._collect_joins(select, alias_map)
   ```

4. **Подсчет JOIN'ов:** в методе `_collect_joins()` для каждого найденного JOIN увеличивается счетчик:
   ```python
   def _collect_joins(self, select_expr: exp.Select, alias_map: Dict[str, Set[str]]) -> None:
       joins = select_expr.args.get("joins") or []
       for join in joins:
           self.count += 1  # ← Здесь увеличивается счетчик
           # ... обработка JOIN'а
   ```

### Важные моменты:

- Счетчик увеличивается **только для JOIN'ов, которые sqlglot успешно распарсил**
- Если JOIN находится в части SQL, которую sqlglot не смог распарсить, он **не будет учтен**
- JOIN'ы из CTE также учитываются, так как они обрабатываются при разборе каждого SELECT

### Пример работы:

Для SQL:
```sql
SELECT * FROM table1
JOIN table2 ON table1.id = table2.id
LEFT JOIN table3 ON table2.id = table3.id
```

Процесс:
1. sqlglot парсит SQL → создает AST
2. Находит SELECT → извлекает список JOIN'ов: `[JOIN table2, LEFT JOIN table3]`
3. Для каждого JOIN вызывается `_collect_joins()`
4. `self.count` увеличивается: 0 → 1 → 2
5. Итоговый результат: `self.count = 2`

---

## 3. Сравнение и статистика

### Где происходит сравнение:

**Расположение:** функция `main()` (строки 374-411)

### Процесс:

```python
def main() -> None:
    total_sqlglot_joins = 0  # Сумма всех JOIN'ов, обработанных sqlglot
    total_regex_joins = 0    # Сумма всех JOIN'ов, найденных regex
    
    for raw_path in args.paths:
        # Создаем экстрактор для каждого файла
        extractor = JoinExtractor(sql_text, source_name=raw_path)
        rows.extend(extractor.extract())
        
        # Суммируем счетчики
        total_sqlglot_joins += extractor.count
        total_regex_joins += extractor.regex_join_count
    
    # Вычисляем процент
    if total_regex_joins > 0:
        percentage = (total_sqlglot_joins / total_regex_joins) * 100
        print(f"Процент обработки: {percentage:.2f}%")
```

### Формула расчета:

```
Процент обработки = (JOIN'ов обработано через sqlglot / JOIN'ов найдено regex) × 100%
```

### Интерпретация результатов:

- **100%** - все JOIN'ы из исходных файлов успешно обработаны sqlglot
- **< 100%** - некоторые JOIN'ы не были распознаны или обработаны парсером
  - Возможные причины:
    - Синтаксические ошибки в SQL
    - Неподдерживаемые конструкции
    - Проблемы с парсингом сложных выражений
- **> 100%** - теоретически возможно, если:
  - Регулярное выражение пропустило некоторые JOIN'ы
  - sqlglot нашел JOIN'ы в подзапросах, которые regex не учел

---

## 4. Пример работы счетчика

### Входной SQL (a1.sql):
```sql
with business_dt as (
    select * from ad_records a
    where a.report_dt = '{dt}'
)
, t as (
    select a1.c_client cft_id
    from business_dt a
    join r2_ac_fin a1 on a1.c_main_v_id = a.acc_ost 
    left join z_r2_acc a2 on a1.c_unique_code = a2.c_unique_code
    left join r2_type_acc ta on ta.id = a2.c_type_acc
)
select * from t
```

### Шаг 1: Подсчет через regex

Регулярное выражение найдет:
- `join r2_ac_fin` → 1
- `left join z_r2_acc` → 1
- `left join r2_type_acc` → 1

**Итого: `regex_join_count = 3`**

### Шаг 2: Подсчет через sqlglot

1. sqlglot парсит SQL
2. Находит CTE `business_dt` и `t`
3. В CTE `t` находит SELECT с 3 JOIN'ами:
   - `join r2_ac_fin`
   - `left join z_r2_acc`
   - `left join r2_type_acc`
4. Для каждого JOIN вызывается `_collect_joins()`
5. `self.count` увеличивается: 0 → 1 → 2 → 3

**Итого: `count = 3`**

### Шаг 3: Вывод статистики

```
============================================================
СТАТИСТИКА ОБРАБОТКИ JOIN'ОВ:
============================================================
JOIN'ов найдено регулярным выражением: 3
JOIN'ов обработано через sqlglot: 3
Процент обработки: 100.00%
============================================================
```

---

## 5. Важные замечания

### Различия в подсчете:

1. **Регулярное выражение:**
   - Ищет JOIN'ы **в исходном тексте** (до парсинга)
   - Может найти JOIN'ы в комментариях (если они не отфильтрованы)
   - Не понимает контекст SQL

2. **sqlglot:**
   - Находит JOIN'ы **только в валидном SQL**
   - Игнорирует JOIN'ы в комментариях
   - Понимает структуру SQL (CTE, подзапросы и т.д.)

### Когда могут быть расхождения:

- **regex > sqlglot:**
  - JOIN'ы в комментариях
  - JOIN'ы в невалидном SQL (который не парсится)
  - JOIN'ы в строках (если они не экранированы)

- **sqlglot > regex:**
  - JOIN'ы в подзапросах, которые regex не учел
  - JOIN'ы, созданные динамически (редко)

### Рекомендации по интерпретации:

- **Процент 100%** - идеальный случай, все JOIN'ы обработаны
- **Процент 90-99%** - нормально, возможны незначительные расхождения
- **Процент < 90%** - стоит проверить, почему многие JOIN'ы не обрабатываются
- **Процент > 100%** - проверить корректность регулярного выражения

---

## 6. Технические детали

### Инициализация счетчиков:

```python
# В __init__ класса JoinExtractor:
self.count = 0  # Счетчик sqlglot (начинается с 0)
self.regex_join_count = count_joins_by_regex(self.sql_text)  # Счетчик regex (вычисляется сразу)
```

### Обновление счетчиков:

```python
# Счетчик sqlglot увеличивается в методе _collect_joins():
def _collect_joins(self, select_expr: exp.Select, alias_map: Dict[str, Set[str]]) -> None:
    joins = select_expr.args.get("joins") or []
    for join in joins:
        self.count += 1  # ← Обновление счетчика
        # ... обработка

# Счетчик regex НЕ обновляется (вычисляется один раз при инициализации)
```

### Агрегация по файлам:

```python
# В функции main():
total_sqlglot_joins = 0
total_regex_joins = 0

for raw_path in args.paths:
    extractor = JoinExtractor(sql_text, source_name=raw_path)
    rows.extend(extractor.extract())
    
    # Суммируем счетчики из каждого файла
    total_sqlglot_joins += extractor.count
    total_regex_joins += extractor.regex_join_count
```

---

## Заключение

Счетчик JOIN'ов позволяет:
1. **Оценить качество обработки** - понять, какой процент JOIN'ов успешно обрабатывается
2. **Выявить проблемы** - если процент низкий, значит есть проблемы с парсингом
3. **Мониторить изменения** - отслеживать, как изменения в SQL влияют на обработку

Это важный инструмент для понимания эффективности работы скрипта и выявления потенциальных проблем с обработкой SQL-файлов.

