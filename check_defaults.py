"""
Скрипт проверки качества данных из источника дефолтов клиентов.

Выполняет 7 блоков проверок:
  0. Общая информация — размер таблицы, список колонок.
  1. Пропуски — количество NULL/пустых значений по каждому полю.
  2. Дубликаты — полные дубли строк, повторы ИНН, анализ множественных дефолтов.
  3. Формат и валидность — корректность ИНН, дат, числовых полей.
  4. Справочные значения — все ли значения входят в допустимые перечни.
  5. Логическая согласованность — перекрёстные проверки дат и полей между собой.
  6. Статистический профиль — распределения по причинам, годам, флагам.

Результат выводится в консоль и сохраняется в файл report_defaults.txt.

Использование:
  python check_defaults.py
"""

import pandas as pd
import sys
import io
import os
from datetime import datetime

# Корень проекта — папка уровнем выше от расположения скрипта
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)

# ============================================================================
# НАСТРОЙКИ — измените под свои данные
# ============================================================================

# Путь к CSV-файлу с данными по дефолтам
FILE_PATH = os.path.join(_PROJECT_ROOT, "sources", "data_defaults.csv")

# Путь, куда будет сохранён текстовый отчёт
REPORT_PATH = os.path.join(_PROJECT_ROOT, "report_defaults.txt")

# Кодировка файла (utf-8-sig для файлов с BOM, cp1251 для старых выгрузок)
ENCODING = "utf-8-sig"

# Разделитель колонок в CSV
SEP = ";"

# Известные причины дефолта — дополните по мере получения информации от владельца данных
KNOWN_DEFAULT_REASONS = {
    "default_90", "def_reserve", "def_bankrupt", "def_restructure",
    "def_cross", "def_judgement", "def_sign_bankrupt",
}

# Формат даты
DATE_FORMAT = "%d.%m.%Y"

# Допустимый диапазон дат: данные о дефолтах с 2009 по 2025 год
DATE_MIN = datetime(2009, 1, 1)
DATE_MAX = datetime(2025, 12, 31)


# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================

def section(title):
    """Печатает заголовок секции отчёта."""
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def parse_date(val):
    """Парсит строку даты, возвращает datetime или None."""
    if pd.isna(val):
        return None
    val = str(val).strip()
    if not val:
        return None
    try:
        return datetime.strptime(val, DATE_FORMAT)
    except ValueError:
        return None


def load_data():
    """
    Загрузка CSV-файла.
    Все колонки читаются как строки (dtype=str), чтобы не потерять ведущие нули
    в ИНН и не допустить автоприведения типов pandas.
    """
    df = pd.read_csv(FILE_PATH, sep=SEP, encoding=ENCODING, dtype=str)
    df.columns = df.columns.str.strip()
    print(f"Загружено строк: {len(df)}, колонок: {len(df.columns)}")
    print(f"Колонки: {list(df.columns)}")
    return df


# ============================================================================
# БЛОК 0. ОБЩАЯ ИНФОРМАЦИЯ
# ============================================================================

def check_shape(df):
    """Выводит общую информацию о таблице: количество строк, колонок и их перечень."""
    section("0. ОБЩАЯ ИНФОРМАЦИЯ")
    print(f"Строк: {len(df)}")
    print(f"Колонок: {len(df.columns)}")
    print(f"Список колонок: {list(df.columns)}")


# ============================================================================
# БЛОК 1. ПРОПУСКИ (NULL / пустые значения)
# ============================================================================

def check_nulls(df):
    """
    Считает количество пропусков (NaN + пустые строки) по каждой колонке.
    Для дефолтов: cure_date и finish_date могут быть пустыми (клиент ещё в дефолте),
    но inn, default_reason, start_date — обязательны.
    """
    section("1. ПРОПУСКИ (NULL / пустые значения)")

    for col in df.columns:
        total = len(df)
        nulls = df[col].isna().sum() + (df[col].astype(str).str.strip() == "").sum()
        pct = nulls / total * 100
        status = "[!]" if pct > 0 else "[OK]"
        if pct > 0:
            print(f"  {status} {col}: {nulls} пропусков ({pct:.1f}%)")
        else:
            print(f"  {status} {col}: нет пропусков")

    # Поля, которые ОБЯЗАТЕЛЬНО должны быть заполнены
    critical = ["inn", "default_reason", "start_date"]
    for col in critical:
        if col not in df.columns:
            continue
        nulls = df[col].isna().sum() + (df[col].astype(str).str.strip() == "").sum()
        if nulls > 0:
            print(f"\n  [!!!] КРИТИЧНО: в поле '{col}' есть {nulls} пропусков -- это обязательное поле!")

    # cure_date и finish_date могут быть пустыми, но выводим аналитику
    if "cure_date" in df.columns:
        empty_cure = df["cure_date"].isna().sum() + (df["cure_date"].astype(str).str.strip() == "").sum()
        print(f"\n  [i] cure_date пуст у {empty_cure} записей ({empty_cure/len(df)*100:.1f}%)")
        print(f"      (допустимо -- клиент может ещё не выйти из дефолта)")

    if "finish_date" in df.columns:
        empty_finish = df["finish_date"].isna().sum() + (df["finish_date"].astype(str).str.strip() == "").sum()
        print(f"  [i] finish_date пуст у {empty_finish} записей ({empty_finish/len(df)*100:.1f}%)")
        print(f"      (допустимо -- дефолт может быть ещё не закрыт)")


# ============================================================================
# БЛОК 2. ДУБЛИКАТЫ
# ============================================================================

def check_duplicates(df):
    """
    Проверяет:
    - Полные дубликаты строк (ошибка загрузки).
    - Повторяющиеся ИНН — ожидаемо, т.к. у клиента может быть несколько дефолтов.
    - Дубликаты по паре inn + start_date (один клиент, одна дата начала — подозрительно).
    """
    section("2. ДУБЛИКАТЫ")

    # --- Полные дубликаты строк ---
    full_dupes = df.duplicated().sum()
    print(f"  Полные дубликаты строк: {full_dupes}")
    if full_dupes > 0:
        print(f"  [!] Найдены полные дубликаты -- вероятна ошибка загрузки")

    # --- Повторяющиеся ИНН ---
    # У одного клиента может быть несколько дефолтов в разное время
    if "inn" in df.columns:
        inn_counts = df["inn"].value_counts()
        total_clients = df["inn"].nunique()
        repeat_clients = (inn_counts > 1).sum()
        print(f"\n  Уникальных ИНН: {total_clients}")
        print(f"  ИНН, встречающихся более 1 раза: {repeat_clients}")
        if repeat_clients > 0:
            print("  (Допустимо -- у клиента может быть несколько дефолтов)")
            print(f"\n  Распределение количества дефолтов на клиента:")
            dist = inn_counts.value_counts().sort_index()
            for cnt, num in dist.items():
                print(f"    {cnt} дефолт(ов): {num} клиентов")

    # --- Дубликаты по паре inn + start_date ---
    # Два дефолта у одного клиента с одной и той же датой начала — подозрительно
    if "inn" in df.columns and "start_date" in df.columns:
        pair_dupes = df.duplicated(subset=["inn", "start_date"]).sum()
        print(f"\n  Дубликаты по паре inn + start_date: {pair_dupes}")
        if pair_dupes > 0:
            print(f"  [?] Два дефолта с одной датой начала у одного клиента -- уточнить допустимость")
            duped = df[df.duplicated(subset=["inn", "start_date"], keep=False)]
            for inn_val in duped["inn"].unique()[:3]:
                subset = duped[duped["inn"] == inn_val]
                print(f"      ИНН {inn_val}: {len(subset)} записей с start_date = {subset['start_date'].iloc[0]}")


# ============================================================================
# БЛОК 3. ФОРМАТ И ВАЛИДНОСТЬ ЗНАЧЕНИЙ
# ============================================================================

def check_formats(df):
    """
    Проверяет корректность формата данных:
    - inn: только цифры, длина 10 или 12.
    - start_date, cure_date, finish_date: корректный формат даты, диапазон 2009-2025.
    - writeoff, unlimited_default: допустимые значения (0/1).
    - sequence_of_defaults: числовые последовательности.
    """
    section("3. ФОРМАТ И ВАЛИДНОСТЬ ЗНАЧЕНИЙ")

    # --- ИНН: только цифры, длина 10 или 12 ---
    if "inn" in df.columns:
        inn_col = df["inn"].dropna().astype(str).str.strip()
        bad_chars = inn_col[~inn_col.str.match(r"^\d+$")]
        bad_len = inn_col[~inn_col.str.len().isin([10, 12])]
        print(f"  ИНН:")
        print(f"    Не только цифры: {len(bad_chars)}")
        print(f"    Длина не 10 и не 12: {len(bad_len)}")
        if len(bad_len) > 0:
            print(f"    Примеры некорректных: {bad_len.head(5).tolist()}")
        len_dist = inn_col.str.len().value_counts().sort_index()
        print(f"    Распределение по длине: {dict(len_dist)}")

    # --- Проверка дат: формат, диапазон ---
    date_cols = ["start_date", "cure_date", "finish_date"]
    for col in date_cols:
        if col not in df.columns:
            continue
        print(f"\n  {col}:")
        date_col = df[col].dropna().astype(str).str.strip()
        date_col = date_col[date_col != ""]
        if len(date_col) == 0:
            print(f"    Все значения пусты")
            continue
        parsed = []
        bad_format = []
        for val in date_col:
            try:
                parsed.append(datetime.strptime(val, DATE_FORMAT))
            except ValueError:
                bad_format.append(val)
        print(f"    Заполнено: {len(date_col)}")
        print(f"    Некорректный формат: {len(bad_format)}")
        if bad_format:
            print(f"    Примеры: {bad_format[:5]}")
        if parsed:
            before_min = [d for d in parsed if d < DATE_MIN]
            after_max = [d for d in parsed if d > DATE_MAX]
            print(f"    Ожидаемый период: {DATE_MIN.strftime(DATE_FORMAT)} -- {DATE_MAX.strftime(DATE_FORMAT)}")
            print(f"    Даты ранее {DATE_MIN.strftime(DATE_FORMAT)}: {len(before_min)}")
            print(f"    Даты позднее {DATE_MAX.strftime(DATE_FORMAT)}: {len(after_max)}")
            dates_sorted = sorted(parsed)
            print(f"    Фактический диапазон: {dates_sorted[0].strftime(DATE_FORMAT)} -- {dates_sorted[-1].strftime(DATE_FORMAT)}")

    # --- writeoff: допустимые значения (ожидаем 0 или 1) ---
    if "writeoff" in df.columns:
        print(f"\n  writeoff (признак списания):")
        wo_col = df["writeoff"].dropna().astype(str).str.strip()
        wo_col = wo_col[wo_col != ""]
        unique_vals = sorted(wo_col.unique())
        print(f"    Уникальные значения: {unique_vals}")
        unexpected = [v for v in unique_vals if v not in ("0", "1")]
        if unexpected:
            print(f"    [!] Неожиданные значения (ожидали 0/1): {unexpected}")
        else:
            print(f"    [OK] Все значения -- 0 или 1")

    # --- unlimited_default: допустимые значения (ожидаем 0 или 1) ---
    if "unlimited_default" in df.columns:
        print(f"\n  unlimited_default (бессрочный дефолт):")
        ud_col = df["unlimited_default"].dropna().astype(str).str.strip()
        ud_col = ud_col[ud_col != ""]
        unique_vals = sorted(ud_col.unique())
        print(f"    Уникальные значения: {unique_vals}")
        unexpected = [v for v in unique_vals if v not in ("0", "1")]
        if unexpected:
            print(f"    [!] Неожиданные значения (ожидали 0/1): {unexpected}")
        else:
            print(f"    [OK] Все значения -- 0 или 1")

    # --- sequence_of_defaults: должна содержать числовые последовательности ---
    if "sequence_of_defaults" in df.columns:
        print(f"\n  sequence_of_defaults:")
        seq_col = df["sequence_of_defaults"].dropna().astype(str).str.strip()
        seq_col = seq_col[seq_col != ""]
        bad_seqs = 0
        for val in seq_col:
            parts = val.split(";")
            for p in parts:
                if not p.strip().isdigit():
                    bad_seqs += 1
                    break
        print(f"    Записей с нечисловыми элементами в последовательности: {bad_seqs}")
        if bad_seqs == 0:
            print(f"    [OK] Все последовательности числовые")


# ============================================================================
# БЛОК 4. СПРАВОЧНЫЕ ЗНАЧЕНИЯ (допустимые перечни)
# ============================================================================

def check_reference_values(df):
    """
    Сверяет фактические значения причин дефолта с известными:
    - default_reason: проверка по KNOWN_DEFAULT_REASONS.
    - reasons_on_last_date_month: аналогичная проверка.
    Перечни неполные — неизвестные значения выводятся для ручного анализа.
    """
    section("4. СПРАВОЧНЫЕ ЗНАЧЕНИЯ (допустимые перечни)")

    # --- Причины дефолта ---
    if "default_reason" in df.columns:
        vals = set(df["default_reason"].dropna().astype(str).str.strip().unique()) - {""}
        unknown = vals - KNOWN_DEFAULT_REASONS
        print(f"  default_reason -- уникальные: {sorted(vals)}")
        if unknown:
            print(f"  [?] Неизвестные причины (отсутствуют в справочнике): {sorted(unknown)}")
            print(f"      Дополните KNOWN_DEFAULT_REASONS при получении расшифровки")
        else:
            print(f"  [OK] Все значения из известного перечня")

        # Распределение по причинам
        print(f"\n  Распределение по причинам дефолта:")
        dist = df["default_reason"].value_counts()
        for val, cnt in dist.items():
            pct = cnt / len(df) * 100
            print(f"    {val:30s} {cnt:>6d} ({pct:5.1f}%)")

    # --- Причины на последнюю дату ---
    if "reasons_on_last_date_month" in df.columns:
        vals = set(df["reasons_on_last_date_month"].dropna().astype(str).str.strip().unique()) - {""}
        unknown = vals - KNOWN_DEFAULT_REASONS
        print(f"\n  reasons_on_last_date_month -- уникальные: {sorted(vals)}")
        if unknown:
            print(f"  [?] Неизвестные причины: {sorted(unknown)}")
        else:
            print(f"  [OK] Все значения из известного перечня")


# ============================================================================
# БЛОК 5. ЛОГИЧЕСКАЯ СОГЛАСОВАННОСТЬ
# ============================================================================

def check_logic(df):
    """
    Перекрёстные проверки — ищем противоречия в данных:
    - cure_date должна быть позже start_date (нельзя выздороветь до начала дефолта).
    - finish_date должна быть >= cure_date (дефолт закрывается после/одновременно с выздоровлением).
    - Если unlimited_default=1, то cure_date и finish_date должны быть пустыми.
    - Если unlimited_default=0, то cure_date и finish_date ожидаемо заполнены.
    - Количество элементов в sequence_of_defaults должно совпадать с sequence_of_dates.
    - writeoff=1 при unlimited_default=0 — нетипично (списание при закрытом дефолте).
    """
    section("5. ЛОГИЧЕСКАЯ СОГЛАСОВАННОСТЬ")

    # --- cure_date > start_date ---
    if "start_date" in df.columns and "cure_date" in df.columns:
        problems = 0
        examples = []
        for _, row in df.iterrows():
            sd = parse_date(row.get("start_date"))
            cd = parse_date(row.get("cure_date"))
            if sd and cd and cd < sd:
                problems += 1
                if len(examples) < 5:
                    examples.append((row.get("inn"), row.get("start_date"), row.get("cure_date")))
        print(f"  cure_date ранее start_date: {problems}")
        if examples:
            for inn, sd, cd in examples:
                print(f"    ИНН {inn}: start={sd}, cure={cd}")
        if problems == 0:
            print(f"    [OK] Все cure_date >= start_date")

    # --- finish_date >= cure_date ---
    if "cure_date" in df.columns and "finish_date" in df.columns:
        problems = 0
        examples = []
        for _, row in df.iterrows():
            cd = parse_date(row.get("cure_date"))
            fd = parse_date(row.get("finish_date"))
            if cd and fd and fd < cd:
                problems += 1
                if len(examples) < 5:
                    examples.append((row.get("inn"), row.get("cure_date"), row.get("finish_date")))
        print(f"\n  finish_date ранее cure_date: {problems}")
        if examples:
            for inn, cd, fd in examples:
                print(f"    ИНН {inn}: cure={cd}, finish={fd}")
        if problems == 0:
            print(f"    [OK] Все finish_date >= cure_date")

    # --- Если unlimited_default=1, то cure_date и finish_date должны быть пустыми ---
    if "unlimited_default" in df.columns:
        ud_rows = df[df["unlimited_default"].astype(str).str.strip() == "1"]
        if len(ud_rows) > 0:
            if "cure_date" in df.columns:
                filled_cure = ud_rows["cure_date"].dropna().astype(str).str.strip()
                filled_cure = filled_cure[filled_cure != ""]
                print(f"\n  unlimited_default=1, но cure_date заполнена: {len(filled_cure)}")
                if len(filled_cure) > 0:
                    print(f"    [?] Бессрочный дефолт, но указана дата выздоровления -- противоречие")
                else:
                    print(f"    [OK] У бессрочных дефолтов cure_date пуста")

            if "finish_date" in df.columns:
                filled_finish = ud_rows["finish_date"].dropna().astype(str).str.strip()
                filled_finish = filled_finish[filled_finish != ""]
                print(f"  unlimited_default=1, но finish_date заполнена: {len(filled_finish)}")
                if len(filled_finish) > 0:
                    print(f"    [?] Бессрочный дефолт, но указана дата окончания -- противоречие")
                else:
                    print(f"    [OK] У бессрочных дефолтов finish_date пуста")

    # --- Если unlimited_default=0, то cure_date ожидаемо заполнена ---
    if "unlimited_default" in df.columns and "cure_date" in df.columns:
        not_ud_rows = df[df["unlimited_default"].astype(str).str.strip() == "0"]
        if len(not_ud_rows) > 0:
            empty_cure = not_ud_rows["cure_date"].isna().sum() + \
                         (not_ud_rows["cure_date"].astype(str).str.strip() == "").sum()
            print(f"\n  unlimited_default=0, но cure_date пуста: {empty_cure}")
            if empty_cure > 0:
                print(f"    [?] Дефолт не бессрочный, но нет даты выздоровления -- уточнить")
            else:
                print(f"    [OK] У всех небессрочных дефолтов cure_date заполнена")

    # --- Количество элементов в sequence_of_defaults = количеству дат в sequence_of_dates ---
    if "sequence_of_defaults" in df.columns and "sequence_of_dates" in df.columns:
        mismatches = 0
        examples = []
        for _, row in df.iterrows():
            seq_d = str(row.get("sequence_of_defaults", "")).strip()
            seq_dt = str(row.get("sequence_of_dates", "")).strip()
            if not seq_d or not seq_dt:
                continue
            cnt_d = len(seq_d.split(";"))
            cnt_dt = len(seq_dt.split(";"))
            if cnt_d != cnt_dt:
                mismatches += 1
                if len(examples) < 5:
                    examples.append((row.get("inn"), cnt_d, cnt_dt))
        print(f"\n  Несовпадение длины sequence_of_defaults и sequence_of_dates: {mismatches}")
        if examples:
            for inn, cd, cdt in examples:
                print(f"    ИНН {inn}: шагов={cd}, дат={cdt}")
        if mismatches == 0:
            print(f"    [OK] Длины последовательностей совпадают")

    # --- writeoff=1 при unlimited_default=0 — нетипично ---
    if "writeoff" in df.columns and "unlimited_default" in df.columns:
        wo_not_ud = df[
            (df["writeoff"].astype(str).str.strip() == "1") &
            (df["unlimited_default"].astype(str).str.strip() == "0")
        ]
        print(f"\n  writeoff=1 при unlimited_default=0: {len(wo_not_ud)}")
        if len(wo_not_ud) > 0:
            print(f"    [?] Списание при закрытом дефолте -- нетипично, стоит проверить")
            for _, row in wo_not_ud.head(3).iterrows():
                print(f"    ИНН {row.get('inn')}: reason={row.get('default_reason')}, "
                      f"start={row.get('start_date')}, cure={row.get('cure_date')}")
        else:
            print(f"    [OK] Все списания -- у бессрочных дефолтов")


# ============================================================================
# БЛОК 6. СТАТИСТИЧЕСКИЙ ПРОФИЛЬ
# ============================================================================

def check_statistics(df):
    """
    Выводит общую статистику для выявления аномалий:
    - Распределение по причинам дефолта.
    - Распределение по годам начала дефолта.
    - Статистика по флагам writeoff и unlimited_default.
    - Распределение длительности дефолтов.
    - Длина последовательностей дефолтов.
    """
    section("6. СТАТИСТИЧЕСКИЙ ПРОФИЛЬ")

    # --- Распределение по причинам дефолта ---
    if "default_reason" in df.columns:
        print("  Распределение по причинам дефолта:")
        dist = df["default_reason"].value_counts()
        for val, cnt in dist.items():
            pct = cnt / len(df) * 100
            bar = "#" * int(pct / 2)
            print(f"    {val:30s} {cnt:>6d} ({pct:5.1f}%) {bar}")

    # --- Распределение по годам начала дефолта ---
    if "start_date" in df.columns:
        print(f"\n  Распределение по годам начала дефолта:")
        dates_parsed = pd.to_datetime(df["start_date"], format=DATE_FORMAT, errors="coerce")
        year_dist = dates_parsed.dt.year.value_counts().sort_index()
        for year, cnt in year_dist.items():
            if pd.notna(year):
                print(f"    {int(year)}: {cnt}")

    # --- Статистика по writeoff ---
    if "writeoff" in df.columns:
        print(f"\n  Статистика по writeoff (списание):")
        wo_dist = df["writeoff"].astype(str).str.strip().value_counts()
        for val, cnt in wo_dist.items():
            label = "списано" if val == "1" else "не списано" if val == "0" else val
            print(f"    {val} ({label}): {cnt} ({cnt/len(df)*100:.1f}%)")

    # --- Статистика по unlimited_default ---
    if "unlimited_default" in df.columns:
        print(f"\n  Статистика по unlimited_default (бессрочный дефолт):")
        ud_dist = df["unlimited_default"].astype(str).str.strip().value_counts()
        for val, cnt in ud_dist.items():
            label = "бессрочный" if val == "1" else "закрытый" if val == "0" else val
            print(f"    {val} ({label}): {cnt} ({cnt/len(df)*100:.1f}%)")

    # --- Длительность дефолтов (от start_date до cure_date) ---
    if "start_date" in df.columns and "cure_date" in df.columns:
        print(f"\n  Длительность дефолтов (start_date -> cure_date):")
        durations = []
        for _, row in df.iterrows():
            sd = parse_date(row.get("start_date"))
            cd = parse_date(row.get("cure_date"))
            if sd and cd and cd >= sd:
                durations.append((cd - sd).days)
        if durations:
            avg_d = sum(durations) / len(durations)
            med_d = sorted(durations)[len(durations)//2]
            print(f"    Рассчитано для: {len(durations)} записей (с заполненной cure_date)")
            print(f"    Средняя: {avg_d:.0f} дней")
            print(f"    Медиана: {med_d} дней")
            print(f"    Минимум: {min(durations)} дней")
            print(f"    Максимум: {max(durations)} дней")
        else:
            print(f"    Нет данных для расчёта")

    # --- Длина последовательностей дефолтов ---
    if "sequence_of_defaults" in df.columns:
        print(f"\n  Длина последовательностей дефолтов:")
        seq_lens = []
        for val in df["sequence_of_defaults"].dropna().astype(str).str.strip():
            if val:
                seq_lens.append(len(val.split(";")))
        if seq_lens:
            from collections import Counter
            len_dist = Counter(seq_lens)
            for length in sorted(len_dist.keys()):
                print(f"    {length} шаг(ов): {len_dist[length]} записей")


# ============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ============================================================================

def main():
    """Запускает все проверки последовательно."""
    print("=" * 70)
    print("  ПРОВЕРКА КАЧЕСТВА ДАННЫХ: ИСТОЧНИК ДЕФОЛТОВ")
    print(f"  Файл: {FILE_PATH}")
    print(f"  Дата проверки: {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print("=" * 70)

    df = load_data()

    check_shape(df)             # 0. Общая информация
    check_nulls(df)             # 1. Пропуски
    check_duplicates(df)        # 2. Дубликаты
    check_formats(df)           # 3. Формат и валидность
    check_reference_values(df)  # 4. Справочные перечни
    check_logic(df)             # 5. Логическая согласованность
    check_statistics(df)        # 6. Статистика

    section("ПРОВЕРКА ЗАВЕРШЕНА")
    print("  Проанализируйте результаты выше и при необходимости")
    print("  скорректируйте допустимые перечни в начале скрипта.")


# ============================================================================
# ТОЧКА ВХОДА
# ============================================================================

if __name__ == "__main__":
    old_stdout = sys.stdout
    buf = io.StringIO()
    sys.stdout = buf
    main()
    sys.stdout = old_stdout
    report = buf.getvalue()
    print(report)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"\nОтчёт сохранён в {REPORT_PATH}")
