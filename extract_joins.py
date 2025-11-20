import argparse
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import sqlglot
from sqlglot import expressions as exp


DIALECT = "oracle"


def strip_placeholders(sql_text: str) -> str:
    """Заменяем плейсхолдеры {table_name} на table_name, чтобы SQL оставался валидным."""
    return re.sub(r"\{([^}]+)\}", r"\1", sql_text)


def count_joins_by_regex(sql_text: str) -> int:
    """
    Подсчитывает количество JOIN'ов в SQL тексте при помощи регулярного выражения.
    Ищет различные типы JOIN: INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL JOIN, просто JOIN.
    """
    # Паттерн для поиска JOIN'ов (case-insensitive)
    # Ищем: JOIN, INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL JOIN, LEFT OUTER JOIN и т.д.
    join_pattern = r'\b(?:INNER\s+)?JOIN\b|\bLEFT\s+(?:OUTER\s+)?JOIN\b|\bRIGHT\s+(?:OUTER\s+)?JOIN\b|\bFULL\s+(?:OUTER\s+)?JOIN\b'
    matches = re.findall(join_pattern, sql_text, re.IGNORECASE)
    return len(matches)


class JoinExtractor:
    """
    Обёртка вокруг sqlglot, которая:
    1) парсит SQL,
    2) строит карту соответствий CTE -> исходные таблицы,
    3) собирает все JOIN-ы и выводит их в виде плоской таблицы.
    """

    def __init__(self, sql_text: str, source_name: str) -> None:
        # Оригинальный SQL плюс имя файла (для сообщений об ошибках)
        self.sql_text = strip_placeholders(sql_text)
        self.source_name = source_name
        # direct_sources: имя CTE -> множество "сырьевых" таблиц
        self.direct_sources: Dict[str, Set[str]] = defaultdict(set)
        # Кэш для уже развёрнутых алиасов, чтобы не обходить граф повторно
        self._resolved_cache: Dict[str, Set[str]] = {}
        # Кэш для _tables_from_select (по id объекта SELECT)
        self._select_tables_cache: Dict[int, Set[str]] = {}
        # Кэш для _tables_from_relation (по id объекта)
        self._relation_tables_cache: Dict[int, Set[str]] = {}
        self.results: List[Dict[str, str]] = []
        self._trees: List[exp.Expression] = []
        # Список уже обработанных SELECT (по id), чтобы избежать дубликатов
        self._processed_selects: Set[int] = set()
        # Счетчик JOIN'ов, обработанных через sqlglot
        self.count = 0
        # Количество JOIN'ов, найденных регулярным выражением
        self.regex_join_count = count_joins_by_regex(self.sql_text)

    def extract(self) -> List[Dict[str, str]]:
        """
        Главная точка входа:
        - пытаемся распарсить SQL (с патчем “select * from последняя_cte” для скриптов,
          которые состоят только из CTE),
        - собираем карту источников,
        - проходим по каждому SELECT и забираем оттуда JOIN-ы.
        """
        try:
            self._trees = sqlglot.parse(self.sql_text, read=DIALECT)
        except sqlglot.errors.ParseError as err:
            patched = self._maybe_patch_cte_only_sql()
            if patched:
                try:
                    self._trees = sqlglot.parse(patched, read=DIALECT)
                    self.sql_text = patched
                except sqlglot.errors.ParseError:
                    raise ValueError(f"Не удалось распарсить SQL в {self.source_name}: {err}") from err
            else:
                raise ValueError(f"Не удалось распарсить SQL в {self.source_name}: {err}") from err

        # Шаг 1: запоминаем, из каких таблиц строится каждый CTE
        self._collect_direct_sources()

        # Шаг 2: собираем все SELECT один раз (оптимизация: избегаем множественных обходов дерева)
        all_selects: List[exp.Select] = []
        for tree in self._trees:
            all_selects.extend(tree.find_all(exp.Select))

        # Шаг 3: обрабатываем каждый SELECT только один раз
        for select in all_selects:
            select_id = id(select)
            if select_id in self._processed_selects:
                continue
            self._processed_selects.add(select_id)
            alias_map = self._build_alias_map(select)
            self._collect_joins(select, alias_map)

        return self.results

    def _collect_direct_sources(self) -> None:
        """Для каждого CTE узнаем список таблиц, которые встречаются в его FROM/JOIN."""
        # Оптимизация: собираем все CTE один раз
        all_ctes: List[exp.CTE] = []
        for tree in self._trees:
            all_ctes.extend(tree.find_all(exp.CTE))
        
        for cte in all_ctes:
            name = (cte.alias_or_name or "").lower()
            if not name:
                continue
            sources = self._tables_from_select(cte.this)
            if sources:
                self.direct_sources[name].update(sources)

    def _tables_from_select(self, select_expr: exp.Select) -> Set[str]:
        """Возвращаем множество таблиц, встречающихся в SELECT (как в FROM, так и в JOIN)."""
        # Оптимизация: кэшируем результат по id объекта SELECT
        select_id = id(select_expr)
        if select_id in self._select_tables_cache:
            return self._select_tables_cache[select_id]

        sources: Set[str] = set()
        from_clause = self._get_from_clause(select_expr)
        if from_clause and from_clause.this:
            sources.update(self._tables_from_relation(from_clause.this))

        for join in select_expr.args.get("joins") or []:
            sources.update(self._tables_from_relation(join.this))
        
        # Сохраняем в кэш
        self._select_tables_cache[select_id] = sources
        return sources

    def _tables_from_relation(self, relation: exp.Expression) -> Set[str]:
        """
        Универсальный разбор "источника данных".
        Источником может быть обычная таблица либо подзапрос; подзапрос разбираем рекурсивно.
        """
        # Оптимизация: кэшируем результат по id объекта relation
        relation_id = id(relation)
        if relation_id in self._relation_tables_cache:
            return self._relation_tables_cache[relation_id]

        result: Set[str] = set()
        if isinstance(relation, exp.Table):
            result = {relation.name}
        elif isinstance(relation, exp.Subquery):
            inner = relation.this
            if isinstance(inner, exp.Select):
                result = self._tables_from_select(inner)
        
        # Сохраняем в кэш
        self._relation_tables_cache[relation_id] = result
        return result

    def _resolve_table(self, name: str, visited: Optional[Set[str]] = None) -> Set[str]:
        """
        Разворачиваем алиас/CTE до физических таблиц.
        Если имя — это CTE, рекурсивно идём к его источникам,
        иначе считаем, что это уже настоящая таблица.
        
        Оптимизация: добавлена защита от циклических зависимостей через параметр visited.
        """
        normalized = name.lower()
        
        # Проверка кэша (быстрый путь)
        if normalized in self._resolved_cache:
            return self._resolved_cache[normalized]

        # Защита от циклических зависимостей
        if visited is None:
            visited = set()
        
        if normalized in visited:
            # Обнаружен цикл - возвращаем имя как есть, чтобы избежать бесконечной рекурсии
            result = {name}
            self._resolved_cache[normalized] = result
            return result
        
        # Добавляем текущий узел в путь обхода
        visited.add(normalized)

        if normalized in self.direct_sources:
            resolved: Set[str] = set()
            for source in self.direct_sources[normalized]:
                # Передаём visited дальше для отслеживания пути
                resolved.update(self._resolve_table(source, visited.copy()))
            self._resolved_cache[normalized] = resolved
            return resolved

        result = {name}
        self._resolved_cache[normalized] = result
        return result

    def _register_table(self, alias_map: Dict[str, Set[str]], table_expr: exp.Expression) -> None:
        """
        Добавляем в карту alias_map соответствия:
        - исходное имя таблицы -> набор базовых таблиц,
        - алиас -> тот же набор.
        """
        if isinstance(table_expr, exp.Table):
            alias = table_expr.alias_or_name
            table_name = table_expr.name
            resolved = self._resolve_table(table_name)
            self._add_alias(alias_map, table_name, resolved)
            if alias:
                self._add_alias(alias_map, alias, resolved)
        elif isinstance(table_expr, exp.Subquery):
            alias = table_expr.alias
            if alias:
                alias_name = alias.alias_or_name
                if alias_name:
                    resolved = self._tables_from_select(table_expr.this)
                    expanded = set()
                    for name in resolved:
                        expanded.update(self._resolve_table(name))
                    self._add_alias(alias_map, alias_name, expanded)

    @staticmethod
    def _add_alias(alias_map: Dict[str, Set[str]], key: Optional[str], tables: Iterable[str]) -> None:
        """Просто добавляем соответствие алиас -> множество таблиц."""
        if not key:
            return
        normalized = key.lower()
        alias_map.setdefault(normalized, set()).update(tables)

    @staticmethod
    def _get_from_clause(select_expr: exp.Select) -> Optional[exp.From]:
        """sqlglot в разных диалектах кладёт FROM в from или from_. Забираем то, что есть."""
        return select_expr.args.get("from") or select_expr.args.get("from_")

    def _build_alias_map(self, select_expr: exp.Select) -> Dict[str, Set[str]]:
        """Строим карту алиасов для одного SELECT (FROM + все JOIN targets)."""
        alias_map: Dict[str, Set[str]] = {}
        from_clause = self._get_from_clause(select_expr)
        if from_clause and from_clause.this:
            self._register_table(alias_map, from_clause.this)

        for join in select_expr.args.get("joins") or []:
            self._register_table(alias_map, join.this)
        return alias_map

    def _collect_joins(self, select_expr: exp.Select, alias_map: Dict[str, Set[str]]) -> None:
        """Проходим по JOIN-ам и преобразуем каждую ON-условие в строку результата."""
        joins = select_expr.args.get("joins") or []
        for join in joins:
            self.count += 1
            join_type = (join.args.get("kind") or join.args.get("side") or "INNER").upper()
            on_expr = join.args.get("on")
            pair_conditions: Dict[Tuple[str, str], List[str]] = defaultdict(list)

            # Разворачиваем AND (f1.id = f2.id AND ...) в отдельные выражения
            for condition in self._flatten_conditions(on_expr):
                if isinstance(condition, exp.EQ):
                    left = condition.this
                    right = condition.expression
                    if isinstance(left, exp.Column) and isinstance(right, exp.Column):
                        self._add_condition(pair_conditions, alias_map, left, right)

            if pair_conditions:
                for (left_table, right_table), conds in pair_conditions.items():
                    condition_text = " AND ".join(conds)
                    self.results.append(
                        {
                            "table1": left_table,
                            "table2": right_table,
                            "join_type": join_type,
                            "condition": condition_text,
                        }
                    )
            else:
                # Если не смогли распарсить выражение (например, сложные функции),
                # выводим исходный ON как есть и стараемся угадать таблицы.
                condition_text = on_expr.sql(dialect=DIALECT) if on_expr else ""
                right_tables = self._tables_from_join_target(join.this)
                left_tables = self._infer_left_tables(on_expr, alias_map)
                for left_table in left_tables or {"<unknown>"}:
                    for right_table in right_tables or {"<unknown>"}:
                        self.results.append(
                            {
                                "table1": left_table,
                                "table2": right_table,
                                "join_type": join_type,
                                "condition": condition_text,
                            }
                        )

    def _add_condition(
        self,
        pair_conditions: Dict[Tuple[str, str], List[str]],
        alias_map: Dict[str, Set[str]],
        left: exp.Column,
        right: exp.Column,
    ) -> None:
        """Превращаем f.id = b.id в набор “физических” таблиц и строк вида table.col = table.col."""
        left_alias = (left.table or "").lower()
        right_alias = (right.table or "").lower()
        left_tables = alias_map.get(left_alias) or {left.table or left.sql()}
        right_tables = alias_map.get(right_alias) or {right.table or right.sql()}
        left_col = left.name
        right_col = right.name

        for lt in sorted(left_tables):
            for rt in sorted(right_tables):
                pair_conditions[(lt, rt)].append(f"{lt}.{left_col} = {rt}.{right_col}")

    @staticmethod
    def _flatten_conditions(expr: Optional[exp.Expression]) -> List[exp.Expression]:
        """Рекурсивно разбиваем (cond1 AND cond2 ...) на отдельные выражения."""
        if expr is None:
            return []
        if isinstance(expr, exp.Paren):
            return JoinExtractor._flatten_conditions(expr.this)
        if isinstance(expr, exp.And):
            return JoinExtractor._flatten_conditions(expr.this) + JoinExtractor._flatten_conditions(expr.expression)
        return [expr]

    def _tables_from_join_target(self, target: exp.Expression) -> Set[str]:
        """
        Узнаём, какие таблицы стоят во второй половине JOIN (справа).
        Нужна для fallback-варианта, когда мы не смогли распарсить ON.
        """
        if isinstance(target, exp.Table):
            return self._resolve_table(target.name)
        if isinstance(target, exp.Subquery):
            resolved = self._tables_from_select(target.this)
            expanded: Set[str] = set()
            for name in resolved:
                expanded.update(self._resolve_table(name))
            return expanded
        return set()

    def _infer_left_tables(self, on_expr: Optional[exp.Expression], alias_map: Dict[str, Set[str]]) -> Set[str]:
        """Пробуем понять, какие таблицы участвуют в ON со стороны “left”, чтобы заполнить таблицу1."""
        tables: Set[str] = set()
        if not on_expr:
            return tables
        for column in on_expr.find_all(exp.Column):
            alias = (column.table or "").lower()
            if alias in alias_map:
                tables.update(alias_map[alias])
        return tables

    def _maybe_patch_cte_only_sql(self) -> Optional[str]:
        """
        Если файл состоит только из CTE (без завершающего SELECT), sqlglot падает.
        Добавляем фиктивный `select * from последняя_cte`, чтобы parser был довольным.
        """
        cte_names = re.findall(r"(?i)\b([A-Z0-9_]+)\s+AS\s*\(", self.sql_text)
        if not cte_names:
            return None
        last_cte = cte_names[-1]
        patched_sql = f"{self.sql_text}\nselect * from {last_cte}"
        return patched_sql


def parse_args() -> argparse.Namespace:
    """CLI-обёртка: читаем пути до файлов, опциональную кодировку и выходной CSV."""
    parser = argparse.ArgumentParser(description="Извлечь связи JOIN из SQL файлов.")
    parser.add_argument("paths", nargs="+", help="Список SQL файлов для анализа.")
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="Кодировка входных файлов (по умолчанию utf-8).",
    )
    parser.add_argument(
        "--output",
        default="joins_result.csv",
        help="Путь к CSV-файлу с результатом (по умолчанию joins_result.csv).",
    )
    return parser.parse_args()


def main() -> None:
    """Читаем каждый файл, запускаем пайплайн и записываем строки в CSV."""
    args = parse_args()
    rows: List[Dict[str, str]] = []
    # Счетчики для статистики
    total_sqlglot_joins = 0
    total_regex_joins = 0

    for raw_path in args.paths:
        path = Path(raw_path)
        if not path.exists():
            raise FileNotFoundError(f"Файл {raw_path} не найден")
        sql_text = path.read_text(encoding=args.encoding)
        extractor = JoinExtractor(sql_text, source_name=raw_path)
        rows.extend(extractor.extract())
        total_sqlglot_joins += extractor.count
        total_regex_joins += extractor.regex_join_count

    header = "Таблица1;Таблица2;Тип связи;Связь1"
    output_path = Path(args.output)
    output_path.write_text(header + "\n", encoding="utf-8")
    with output_path.open("a", encoding="utf-8") as outfile:
        for row in rows:
            outfile.write(f"{row['table1']};{row['table2']};{row['join_type']};{row['condition']}\n")
    print(f"Готово. Результат сохранён в {output_path.resolve()}")

    # Выводим статистику обработки JOIN'ов
    print("\n" + "="*60)
    print("СТАТИСТИКА ОБРАБОТКИ JOIN'ОВ:")
    print("="*60)
    print(f"JOIN'ов найдено регулярным выражением: {total_regex_joins}")
    print(f"JOIN'ов обработано через sqlglot: {total_sqlglot_joins}")
    if total_regex_joins > 0:
        percentage = (total_sqlglot_joins / total_regex_joins) * 100
        print(f"Процент обработки: {percentage:.2f}%")
    else:
        print("Процент обработки: N/A (не найдено JOIN'ов в исходных файлах)")
    print("="*60)


if __name__ == "__main__":
    main()

