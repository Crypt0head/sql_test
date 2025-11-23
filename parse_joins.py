"""
Парсер SQL файлов для извлечения JOIN'ов между таблицами.
Обрабатывает CTE, разворачивает их до исходных таблиц, игнорирует дополнительные условия.
"""
import argparse
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import sqlglot
from sqlglot import expressions as exp


def count_joins_by_regex(sql_text: str) -> int:
    """
    Подсчитывает количество JOIN'ов в SQL тексте при помощи регулярного выражения.
    Ищет различные типы JOIN: INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL JOIN, просто JOIN.
    """
    join_pattern = r'\b(?:INNER\s+)?JOIN\b|\bLEFT\s+(?:OUTER\s+)?JOIN\b|\bRIGHT\s+(?:OUTER\s+)?JOIN\b|\bFULL\s+(?:OUTER\s+)?JOIN\b'
    matches = re.findall(join_pattern, sql_text, re.IGNORECASE)
    return len(matches)


def strip_placeholders(sql_text: str) -> str:
    """Заменяем плейсхолдеры {table_name} на table_name, чтобы SQL оставался валидным."""
    return re.sub(r"\{([^}]+)\}", r"\1", sql_text)


class JoinParser:
    """
    Парсер для извлечения JOIN'ов из SQL файлов.
    Обрабатывает CTE, разворачивает их до исходных таблиц.
    """
    
    def __init__(self, sql_text: str, source_name: str):
        self.sql_text = strip_placeholders(sql_text)
        self.source_name = source_name
        # Карта CTE -> множество исходных таблиц
        self.cte_sources: Dict[str, Set[str]] = {}
        # Карта CTE -> содержит ли CTE JOIN'ы (для игнорирования производных CTE)
        self.cte_has_joins: Dict[str, bool] = {}
        # Кэш для развёрнутых CTE
        self._resolved_cache: Dict[str, Set[str]] = {}
        # Результаты парсинга
        self.results: List[Dict[str, str]] = []
        # Счетчики
        self.regex_join_count = count_joins_by_regex(self.sql_text)
        self.processed_join_count = 0
        self.parsed_trees: List[exp.Expression] = []
        
    def parse(self) -> List[Dict[str, str]]:
        """Главный метод парсинга."""
        try:
            self.parsed_trees = sqlglot.parse(self.sql_text, read="oracle")
        except sqlglot.errors.ParseError as err:
            # Пробуем добавить фиктивный SELECT для файлов только с CTE
            patched = self._patch_cte_only_sql()
            if patched:
                try:
                    self.parsed_trees = sqlglot.parse(patched, read="oracle")
                    self.sql_text = patched
                except sqlglot.errors.ParseError:
                    raise ValueError(f"Не удалось распарсить SQL в {self.source_name}: {err}") from err
            else:
                raise ValueError(f"Не удалось распарсить SQL в {self.source_name}: {err}") from err
        
        # Шаг 1: Собираем информацию о CTE и их источниках
        self._collect_cte_info()
        
        # Шаг 2: Извлекаем JOIN'ы из всех SELECT
        self._extract_joins()
        
        return self.results
    
    def _patch_cte_only_sql(self) -> Optional[str]:
        """Добавляет фиктивный SELECT для файлов только с CTE."""
        cte_names = re.findall(r"(?i)\b([A-Z0-9_]+)\s+AS\s*\(", self.sql_text)
        if not cte_names:
            return None
        last_cte = cte_names[-1]
        return f"{self.sql_text}\nSELECT * FROM {last_cte}"
    
    def _collect_cte_info(self) -> None:
        """Собирает информацию о CTE: источники таблиц и наличие JOIN'ов."""
        all_ctes: List[exp.CTE] = []
        for tree in self.parsed_trees:
            all_ctes.extend(tree.find_all(exp.CTE))
        
        for cte in all_ctes:
            cte_name = (cte.alias_or_name or "").lower()
            if not cte_name:
                continue
            
            # Получаем таблицы из SELECT внутри CTE
            sources = self._get_tables_from_select(cte.this)
            if sources:
                self.cte_sources[cte_name] = sources
            
            # Проверяем, есть ли в CTE JOIN'ы
            has_joins = len(cte.this.args.get("joins") or []) > 0
            self.cte_has_joins[cte_name] = has_joins
    
    def _get_tables_from_select(self, select_expr: exp.Select) -> Set[str]:
        """Извлекает множество таблиц из SELECT (FROM + JOIN targets)."""
        tables: Set[str] = set()
        
        # Таблица из FROM
        from_clause = select_expr.args.get("from") or select_expr.args.get("from_")
        if from_clause and from_clause.this:
            table_name = self._get_table_name_from_relation(from_clause.this)
            if table_name:
                tables.add(table_name)
        
        # Таблицы из JOIN'ов
        for join in select_expr.args.get("joins") or []:
            table_name = self._get_table_name_from_relation(join.this)
            if table_name:
                tables.add(table_name)
        
        return tables
    
    def _get_table_name_from_relation(self, relation: exp.Expression) -> Optional[str]:
        """Извлекает имя таблицы из relation."""
        if isinstance(relation, exp.Table):
            return relation.name
        elif isinstance(relation, exp.Subquery):
            # Для подзапроса возвращаем None (не разворачиваем здесь)
            return None
        return None
    
    def _resolve_cte(self, cte_name: str, visited: Optional[Set[str]] = None) -> Set[str]:
        """
        Разворачивает CTE до исходных таблиц.
        Разворачивает ТОЛЬКО простые CTE (без JOIN'ов).
        Если CTE содержит JOIN'ы, возвращаем пустое множество (не разворачиваем).
        """
        normalized = cte_name.lower()
        
        # Защита от циклов
        if visited is None:
            visited = set()
        
        if normalized in visited:
            return set()  # Цикл - возвращаем пустое множество
        
        visited.add(normalized)
        
        # Если это CTE
        if normalized in self.cte_sources:
            # Если CTE содержит JOIN'ы - НЕ разворачиваем
            if normalized in self.cte_has_joins and self.cte_has_joins[normalized]:
                return set()  # CTE с JOIN'ами не разворачиваем
            
            # Разворачиваем только простые CTE (без JOIN'ов)
            resolved: Set[str] = set()
            for source in self.cte_sources[normalized]:
                source_lower = source.lower()
                # Если source - это тоже CTE, рекурсивно разворачиваем
                if source_lower in self.cte_sources:
                    resolved.update(self._resolve_cte(source, visited.copy()))
                else:
                    # Это реальная таблица
                    resolved.add(source)
            return resolved
        
        # Не CTE - возвращаем как есть
        return {cte_name}
    
    def _should_process_cte(self, cte_name: str) -> bool:
        """
        Определяет, нужно ли обрабатывать JOIN'ы из CTE.
        Не обрабатываем CTE, которые являются производными от множественных JOIN'ов.
        """
        normalized = cte_name.lower()
        
        # Если CTE содержит JOIN'ы, обрабатываем его
        if normalized in self.cte_has_joins and self.cte_has_joins[normalized]:
            return True
        
        # Если CTE является простой производной (SELECT из другого CTE без JOIN'ов),
        # не обрабатываем его отдельно
        return False
    
    def _extract_joins(self) -> None:
        """Извлекает JOIN'ы из всех SELECT в парсере."""
        all_selects: List[exp.Select] = []
        for tree in self.parsed_trees:
            all_selects.extend(tree.find_all(exp.Select))
        
        # Обрабатываем каждый SELECT
        processed_selects: Set[int] = set()
        for select in all_selects:
            select_id = id(select)
            if select_id in processed_selects:
                continue
            processed_selects.add(select_id)
            
            # Проверяем, нужно ли обрабатывать этот SELECT
            # Если SELECT находится в CTE, который является производной, пропускаем
            if self._is_derived_cte_select(select):
                continue
            
            self._process_select_joins(select)
    
    def _is_derived_cte_select(self, select_expr: exp.Select) -> bool:
        """
        Проверяет, является ли SELECT частью производного CTE
        (который не содержит JOIN'ов, а только SELECT из другого CTE).
        """
        # Получаем FROM таблицу
        from_clause = select_expr.args.get("from") or select_expr.args.get("from_")
        if not from_clause or not from_clause.this:
            return False
        
        from_table = self._get_table_name_from_relation(from_clause.this)
        if not from_table:
            return False
        
        # Если FROM указывает на CTE, который не содержит JOIN'ов
        from_table_lower = from_table.lower()
        if from_table_lower in self.cte_has_joins:
            if not self.cte_has_joins[from_table_lower]:
                # Это производной CTE - не обрабатываем JOIN'ы из него
                # Но если в этом SELECT есть JOIN'ы, обрабатываем их
                joins = select_expr.args.get("joins") or []
                if not joins:
                    return True  # Пропускаем этот SELECT
        
        return False
    
    def _process_select_joins(self, select_expr: exp.Select) -> None:
        """Обрабатывает JOIN'ы в конкретном SELECT."""
        joins = select_expr.args.get("joins") or []
        if not joins:
            return
        
        # Получаем основную таблицу из FROM
        from_clause = select_expr.args.get("from") or select_expr.args.get("from_")
        main_table = None
        if from_clause and from_clause.this:
            main_table = self._get_table_name_from_relation(from_clause.this)
        
        # Строим карту алиасов для определения таблиц в условиях JOIN
        alias_map = self._build_alias_map(select_expr)
        
        for join in joins:
            self.processed_join_count += 1
            join_type = (join.args.get("kind") or join.args.get("side") or "INNER").upper()
            on_expr = join.args.get("on")
            
            # Получаем имя таблицы из JOIN
            join_table = self._get_table_name_from_relation(join.this)
            
            # Проверяем, является ли одна из сторон CTE с JOIN'ами
            # Если да - игнорируем этот JOIN
            main_table_lower = (main_table or "").lower()
            join_table_lower = (join_table or "").lower()
            
            # Если основная таблица - это CTE с JOIN'ами, пропускаем
            if main_table_lower in self.cte_has_joins and self.cte_has_joins[main_table_lower]:
                continue
            
            # Если таблица в JOIN - это CTE с JOIN'ами, пропускаем
            if join_table_lower in self.cte_has_joins and self.cte_has_joins[join_table_lower]:
                continue
            
            # Разворачиваем CTE до исходных таблиц (только простые CTE)
            left_tables = self._resolve_table(main_table) if main_table else set()
            right_tables = self._resolve_table(join_table) if join_table else set()
            
            # Если после развёртывания не получили таблиц - пропускаем
            if not left_tables or not right_tables:
                continue
            
            # Извлекаем основное условие JOIN (игнорируем AND условия)
            join_condition = self._extract_main_join_condition(on_expr, alias_map, main_table, join_table)
            
            if join_condition and left_tables and right_tables:
                left_alias, left_col, right_alias, right_col = join_condition
                
                # Определяем, какая таблица соответствует какому алиасу
                # Определяем реальные таблицы на основе алиасов
                left_table_from_alias = alias_map.get(left_alias) if left_alias else None
                right_table_from_alias = alias_map.get(right_alias) if right_alias else None
                
                # Если алиасы не найдены, используем основные таблицы
                if not left_table_from_alias:
                    left_table_from_alias = main_table
                if not right_table_from_alias:
                    right_table_from_alias = join_table
                
                # Разворачиваем до исходных таблиц
                left_resolved = self._resolve_table(left_table_from_alias) if left_table_from_alias else left_tables
                right_resolved = self._resolve_table(right_table_from_alias) if right_table_from_alias else right_tables
                
                # Если после развёртывания не получили таблиц - пропускаем
                if not left_resolved or not right_resolved:
                    continue
                
                # Создаём запись для каждой пары таблиц
                for left_table in sorted(left_resolved):
                    for right_table in sorted(right_resolved):
                        # Пропускаем случаи, где таблицы одинаковые
                        if left_table == right_table:
                            continue
                        # Формируем условие с правильным порядком таблиц
                        condition = f"{left_table}.{left_col} = {right_table}.{right_col}"
                        self.results.append({
                            "table1": left_table,
                            "table2": right_table,
                            "join_type": join_type,
                            "condition": condition
                        })
    
    def _build_alias_map(self, select_expr: exp.Select) -> Dict[str, str]:
        """Строит карту алиасов: алиас -> имя таблицы."""
        alias_map = {}
        
        # Таблица из FROM
        from_clause = select_expr.args.get("from") or select_expr.args.get("from_")
        if from_clause and from_clause.this:
            table_name = self._get_table_name_from_relation(from_clause.this)
            if table_name:
                alias = self._get_alias(from_clause.this)
                if alias:
                    alias_map[alias.lower()] = table_name
        
        # Таблицы из JOIN'ов
        for join in select_expr.args.get("joins") or []:
            table_name = self._get_table_name_from_relation(join.this)
            if table_name:
                alias = self._get_alias(join.this)
                if alias:
                    alias_map[alias.lower()] = table_name
        
        return alias_map
    
    def _get_alias(self, relation: exp.Expression) -> Optional[str]:
        """Получает алиас из relation."""
        if isinstance(relation, exp.Table):
            return relation.alias
        elif isinstance(relation, exp.Subquery):
            if relation.alias:
                return relation.alias.alias_or_name if isinstance(relation.alias, exp.TableAlias) else None
        return None
    
    def _resolve_table(self, table_name: Optional[str]) -> Set[str]:
        """Разворачивает таблицу/CTE до исходных таблиц."""
        if not table_name:
            return set()
        
        # Проверяем, является ли это CTE
        if table_name.lower() in self.cte_sources:
            return self._resolve_cte(table_name)
        
        # Это реальная таблица
        return {table_name}
    
    def _extract_main_join_condition(
        self, 
        on_expr: Optional[exp.Expression],
        alias_map: Dict[str, str],
        left_table: Optional[str],
        right_table: Optional[str]
    ) -> Optional[Tuple[str, str, str, str]]:
        """
        Извлекает основное условие JOIN (первое равенство).
        Игнорирует дополнительные AND условия.
        Возвращает (left_table_alias, left_column, right_table_alias, right_column).
        """
        if not on_expr:
            return None
        
        # Ищем первое равенство (EQ), игнорируя AND условия
        first_eq = self._find_first_equality(on_expr)
        if not first_eq:
            return None
        
        left = first_eq.this
        right = first_eq.expression
        
        if not isinstance(left, exp.Column) or not isinstance(right, exp.Column):
            return None
        
        # Получаем алиасы и имена колонок
        left_alias = (left.table or "").lower()
        left_col = left.name
        right_alias = (right.table or "").lower()
        right_col = right.name
        
        return (left_alias, left_col, right_alias, right_col)
    
    def _find_first_equality(self, expr: exp.Expression) -> Optional[exp.EQ]:
        """Находит первое равенство в выражении, игнорируя AND условия."""
        if isinstance(expr, exp.EQ):
            return expr
        elif isinstance(expr, exp.And):
            # Возвращаем только левую часть (первое условие)
            return self._find_first_equality(expr.this)
        elif isinstance(expr, exp.Paren):
            return self._find_first_equality(expr.this)
        return None
    


def main():
    """Главная функция."""
    parser = argparse.ArgumentParser(description="Парсер JOIN'ов из SQL файлов")
    parser.add_argument("files", nargs="+", help="SQL файлы для обработки")
    parser.add_argument("--output", "-o", default="joins_result.csv", help="Выходной CSV файл")
    parser.add_argument("--encoding", default="utf-8", help="Кодировка файлов")
    
    args = parser.parse_args()
    
    all_results: List[Dict[str, str]] = []
    total_regex_joins = 0
    total_processed_joins = 0
    
    # Обрабатываем каждый файл
    for file_path in args.files:
        path = Path(file_path)
        if not path.exists():
            print(f"Предупреждение: файл {file_path} не найден, пропускаем")
            continue
        
        print(f"Обработка {file_path}...")
        sql_text = path.read_text(encoding=args.encoding)
        
        join_parser = JoinParser(sql_text, str(file_path))
        results = join_parser.parse()
        
        all_results.extend(results)
        total_regex_joins += join_parser.regex_join_count
        total_processed_joins += join_parser.processed_join_count
        
        print(f"  Найдено JOIN'ов (regex): {join_parser.regex_join_count}")
        print(f"  Обработано JOIN'ов: {join_parser.processed_join_count}")
        print(f"  Извлечено связей: {len(results)}")
    
    # Дедупликация результатов
    unique_results = {}
    for result in all_results:
        key = (result["table1"], result["table2"], result["join_type"], result["condition"])
        if key not in unique_results:
            unique_results[key] = result
    
    unique_list = list(unique_results.values())
    duplicates_count = len(all_results) - len(unique_list)
    
    # Сохраняем в CSV
    output_path = Path(args.output)
    with output_path.open("w", encoding="utf-8") as f:
        f.write("Таблица1;Таблица2;Тип связи;Связь1\n")
        for result in unique_list:
            f.write(f"{result['table1']};{result['table2']};{result['join_type']};{result['condition']}\n")
    
    print(f"\nРезультат сохранён в {output_path}")
    
    # Выводим отчёт
    print("\n" + "="*60)
    print("ОТЧЁТ О РАБОТЕ ПАРСЕРА")
    print("="*60)
    print(f"JOIN'ов найдено регулярным выражением: {total_regex_joins}")
    print(f"JOIN'ов обработано при помощи библиотеки: {total_processed_joins}")
    print(f"Всего извлечено связей: {len(all_results)}")
    print(f"Уникальных связей: {len(unique_list)}")
    print(f"Одинаковых строк (дубликатов): {duplicates_count}")
    print("="*60)


if __name__ == "__main__":
    main()
