"""
Скрипт проверки качества данных из источника H2O (Факторы проблемности клиентов).

Выполняет 7 блоков проверок:
  0. Общая информация — размер таблицы, список колонок.
  1. Пропуски — количество NULL/пустых значений по каждому полю.
  2. Дубликаты — полные дубли строк, дубли по ключевым ID, повторы ИНН.
  3. Формат и валидность — корректность ИНН, дат (период 2023-2024), ref_book_fp_id.
  4. Справочные значения — все ли значения входят в допустимые перечни.
  5. Логическая согласованность — перекрёстные проверки между полями.
  6. Статистический профиль — распределения по сегментам, филиалам, датам.

Результат выводится в консоль и сохраняется в файл report_h20.txt.

Использование:
  python check_h20.py
"""

import pandas as pd
import sys
import io
import os
import re
from datetime import datetime
from collections import Counter

# Корень проекта — папка уровнем выше от расположения скрипта
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)

# ============================================================================
# НАСТРОЙКИ — измените под свои данные
# ============================================================================

# Путь к CSV-файлу с данными H2O
FILE_PATH = os.path.join(_PROJECT_ROOT, "sources", "data_h20.csv")

# Путь, куда будет сохранён текстовый отчёт
REPORT_PATH = os.path.join(_PROJECT_ROOT, "report_h20.txt")

# Кодировка файла (utf-8-sig для файлов с BOM, cp1251 для старых выгрузок)
ENCODING = "utf-8-sig"

# Разделитель колонок в CSV
SEP = ";"

# Допустимые организационно-правовые формы — дополните при необходимости
VALID_LEGAL_FORMS = {
    "ООО", "АО", "ПАО", "НАО", "ЗАО", "ИП", "ГУП", "МУП", "КФХ",
    "АНО", "НКО", "ТСЖ", "ПК", "КТ", "ОДО", "ФГУП", "ОП",
}

# Допустимые значения сегментов бизнеса
VALID_SEGMENTS = {
    "ДМкб", "ДМБ", "ДМ", "ДСБ", "ДКБ", "ДРПА", "Не подлежит сегментации",
}

# Допустимые типы мониторинга (NULL/пустое значение тоже допустимо)
VALID_MON_TYPES = {"Стандартный", "Упрощенный", "Индивидуальный"}

# Оргформы, у которых ИНН должен быть 12-значным (физлица/ИП)
IP_LEGAL_FORMS = {"ИП", "КФХ"}

# Формат даты в поле fp_start_date
DATE_FORMAT = "%d.%m.%Y"

# Границы допустимого диапазона дат: данные должны лежать в пределах 2023-2025 годов
DATE_MIN = datetime(2023, 1, 1)
DATE_MAX = datetime(2025, 12, 31)


# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================

def section(title):
    """Печатает заголовок секции отчёта."""
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


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
    """
    Выводит общую информацию о таблице: количество строк, колонок,
    и перечень найденных колонок.
    """
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
    Отдельно предупреждает, если пропуски есть в критичных полях,
    которые обязательны для идентификации записи:
    inn, h20_fp_id, fp_start_date, ref_book_fp_id.
    """
    section("1. ПРОПУСКИ (NULL / пустые значения)")

    # Проходим по всем колонкам и считаем NaN + пустые строки
    for col in df.columns:
        total = len(df)
        nulls = df[col].isna().sum() + (df[col].astype(str).str.strip() == "").sum()
        pct = nulls / total * 100
        status = "[!]" if pct > 0 else "[OK]"
        if pct > 0:
            print(f"  {status} {col}: {nulls} пропусков ({pct:.1f}%)")
        else:
            print(f"  {status} {col}: нет пропусков")

    # Дополнительная проверка полей, которые ОБЯЗАТЕЛЬНО должны быть заполнены
    critical = ["inn", "h20_fp_id", "fp_start_date", "ref_book_fp_id"]
    for col in critical:
        if col not in df.columns:
            continue
        nulls = df[col].isna().sum() + (df[col].astype(str).str.strip() == "").sum()
        if nulls > 0:
            print(f"\n  [!!!] КРИТИЧНО: в поле '{col}' есть {nulls} пропусков — это обязательное поле!")


# ============================================================================
# БЛОК 2. ДУБЛИКАТЫ
# ============================================================================

def check_duplicates(df):
    """
    Проверяет несколько видов дубликатов:
    - Полные дубликаты строк (все поля совпадают) — признак ошибки загрузки/ETL.
    - Дубликаты h20_fp_id — ID карточки ФП должен быть уникальным.
    - Дубликаты crm_fp_id — ID в CRM тоже должен быть уникальным.
    - Повторяющиеся ИНН — нормально, если у клиента несколько ФП; выводим распределение.
    - Дубликаты по паре inn + ref_book_fp_id — один и тот же фактор у одного клиента.
    - Проверка что одному ИНН соответствует ровно один cdi_id (внутренний ID банка).
    """
    section("2. ДУБЛИКАТЫ")

    # --- Полные дубликаты строк ---
    full_dupes = df.duplicated().sum()
    print(f"  Полные дубликаты строк: {full_dupes}")
    if full_dupes > 0:
        print(f"  [!] Найдены полные дубликаты — вероятна ошибка загрузки")

    # --- Дубликаты по h20_fp_id (ID карточки ФП в системе H2O) ---
    if "h20_fp_id" in df.columns:
        h20_clean = df[df["h20_fp_id"].notna() & (df["h20_fp_id"].str.strip() != "")]
        h20_dupes = h20_clean["h20_fp_id"].duplicated().sum()
        print(f"  Дубликаты h20_fp_id: {h20_dupes}")
        if h20_dupes > 0:
            print(f"  [!] ID карточки ФП должен быть уникальным — найдены дубли")
            duped_ids = h20_clean[h20_clean["h20_fp_id"].duplicated(keep=False)]
            print(f"      Примеры: {duped_ids['h20_fp_id'].unique()[:5].tolist()}")

    # --- Дубликаты по crm_fp_id (ID карточки ФП в CRM) ---
    if "crm_fp_id" in df.columns:
        crm_clean = df[df["crm_fp_id"].notna() & (df["crm_fp_id"].str.strip() != "")]
        crm_dupes = crm_clean["crm_fp_id"].duplicated().sum()
        print(f"  Дубликаты crm_fp_id: {crm_dupes}")
        if crm_dupes > 0:
            print(f"  [!] CRM ID карточки ФП должен быть уникальным — найдены дубли")

    # --- Анализ повторяющихся ИНН ---
    # У одного клиента может быть несколько факторов проблемности,
    # поэтому повтор ИНН — не обязательно ошибка. Но полезно видеть распределение.
    if "inn" in df.columns:
        inn_counts = df["inn"].value_counts()
        total_clients = df["inn"].nunique()
        repeat_clients = (inn_counts > 1).sum()
        print(f"\n  Уникальных ИНН: {total_clients}")
        print(f"  ИНН, встречающихся более 1 раза: {repeat_clients}")
        if repeat_clients > 0:
            print("  (Это может быть нормой — у клиента несколько ФП)")
            print(f"\n  Распределение количества ФП на клиента:")
            fp_dist = inn_counts.value_counts().sort_index()
            for cnt, num in fp_dist.items():
                print(f"    {cnt} ФП: {num} клиентов")
            top5 = inn_counts.head(5)
            print(f"\n  Топ-5 клиентов по количеству ФП:")
            for inn_val, cnt in top5.items():
                name = df[df["inn"] == inn_val]["client_name"].iloc[0]
                print(f"    ИНН {inn_val} ({name}): {cnt} ФП")

    # --- Дубликаты по паре ИНН + фактор проблемности ---
    # Один и тот же фактор у одного клиента дважды — подозрительно
    if "inn" in df.columns and "ref_book_fp_id" in df.columns:
        pair_dupes = df.duplicated(subset=["inn", "ref_book_fp_id"]).sum()
        print(f"\n  Дубликаты по паре inn + ref_book_fp_id: {pair_dupes}")
        if pair_dupes > 0:
            print(f"  [?] Один и тот же фактор у одного клиента — уточнить, допустимо ли это")

    # --- Проверка: одному ИНН должен соответствовать один cdi_id ---
    # Если у одного ИНН несколько cdi_id, значит данные в источниках не согласованы
    if "inn" in df.columns and "cdi_id" in df.columns:
        inn_cdi = df.groupby("inn")["cdi_id"].nunique()
        mismatch = inn_cdi[inn_cdi > 1]
        if len(mismatch) > 0:
            print(f"\n  [!] ИНН с несколькими cdi_id: {len(mismatch)}")
            for inn_val in mismatch.index[:5]:
                ids = df[df["inn"] == inn_val]["cdi_id"].unique().tolist()
                print(f"      ИНН {inn_val}: cdi_id = {ids}")
        else:
            print(f"\n  [OK] Каждому ИНН соответствует ровно один cdi_id")


# ============================================================================
# БЛОК 3. ФОРМАТ И ВАЛИДНОСТЬ ЗНАЧЕНИЙ
# ============================================================================

def check_formats(df):
    """
    Проверяет корректность формата данных в ключевых полях:
    - inn: только цифры, длина 10 (юрлица) или 12 (ИП/физлица).
    - fp_start_date: корректный формат даты, период должен быть в пределах 2023-2024.
    - ref_book_fp_id: должен быть числовым; выводит все уникальные значения.
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

    # --- Даты: формат, диапазон, будущие даты ---
    if "fp_start_date" in df.columns:
        print(f"\n  Даты (fp_start_date):")
        date_col = df["fp_start_date"].dropna().astype(str).str.strip()
        date_col = date_col[date_col != ""]
        parsed = []
        bad_format = []
        for val in date_col:
            try:
                parsed.append(datetime.strptime(val, DATE_FORMAT))
            except ValueError:
                bad_format.append(val)
        print(f"    Некорректный формат: {len(bad_format)}")
        if bad_format:
            print(f"    Примеры: {bad_format[:5]}")
        if parsed:
            # Даты должны лежать в пределах 2023-2024
            after_max = [d for d in parsed if d > DATE_MAX]
            before_min = [d for d in parsed if d < DATE_MIN]
            out_of_range = before_min + after_max
            print(f"    Ожидаемый период: {DATE_MIN.strftime(DATE_FORMAT)} -- {DATE_MAX.strftime(DATE_FORMAT)}")
            print(f"    Даты ранее {DATE_MIN.strftime(DATE_FORMAT)}: {len(before_min)}")
            print(f"    Даты позднее {DATE_MAX.strftime(DATE_FORMAT)}: {len(after_max)}")
            print(f"    Итого за пределами периода: {len(out_of_range)}")
            dates_sorted = sorted(parsed)
            print(f"    Фактический диапазон: {dates_sorted[0].strftime(DATE_FORMAT)} -- {dates_sorted[-1].strftime(DATE_FORMAT)}")

    # --- ref_book_fp_id: числовой ID из справочника факторов ---
    if "ref_book_fp_id" in df.columns:
        print(f"\n  ref_book_fp_id:")
        ref_col = df["ref_book_fp_id"].dropna().astype(str).str.strip()
        ref_col = ref_col[ref_col != ""]
        non_numeric = ref_col[~ref_col.str.match(r"^\d+$")]
        print(f"    Нечисловые значения: {len(non_numeric)}")
        unique_refs = sorted(ref_col.unique(), key=lambda x: int(x) if x.isdigit() else 0)
        print(f"    Уникальных значений: {len(unique_refs)}")
        if len(unique_refs) <= 100:
            print(f"    Значения: {unique_refs}")


# ============================================================================
# БЛОК 4. СПРАВОЧНЫЕ ЗНАЧЕНИЯ (допустимые перечни)
# ============================================================================

def check_reference_values(df):
    """
    Сверяет фактические значения в справочных полях с допустимыми перечнями:
    - legal_form: проверка по VALID_LEGAL_FORMS
    - segment: проверка по VALID_SEGMENTS
    - mon_type: проверка по VALID_MON_TYPES
    - rf: выводит список всех филиалов с количеством записей (для ручной проверки)
    """
    section("4. СПРАВОЧНЫЕ ЗНАЧЕНИЯ (допустимые перечни)")

    # --- Организационно-правовая форма ---
    if "legal_form" in df.columns:
        vals = set(df["legal_form"].dropna().astype(str).str.strip().unique())
        unknown = vals - VALID_LEGAL_FORMS - {""}
        print(f"  legal_form -- уникальные: {sorted(vals - {''})}")
        if unknown:
            print(f"  [!] Неизвестные формы: {unknown}")
        else:
            print(f"  [OK] Все значения из допустимого перечня")

    # --- Сегмент бизнеса ---
    if "segment" in df.columns:
        vals = set(df["segment"].dropna().astype(str).str.strip().unique())
        unknown = vals - VALID_SEGMENTS - {""}
        print(f"\n  segment -- уникальные: {sorted(vals - {''})}")
        if unknown:
            print(f"  [!] Неизвестные сегменты: {unknown}")
        else:
            print(f"  [OK] Все значения из допустимого перечня")

    # --- Тип мониторинга ---
    if "mon_type" in df.columns:
        vals = set(df["mon_type"].dropna().astype(str).str.strip().unique())
        unknown = vals - VALID_MON_TYPES - {""}
        print(f"\n  mon_type -- уникальные: {sorted(vals - {''})}")
        if unknown:
            print(f"  [!] Неизвестные типы мониторинга: {unknown}")
        else:
            print(f"  [OK] Все значения из допустимого перечня")

    # --- Региональные филиалы ---
    # Справочника филиалов нет, поэтому просто выводим все уникальные значения
    # с количеством записей — для ручной проверки на опечатки и лишние значения
    if "rf" in df.columns:
        vals = sorted(df["rf"].dropna().astype(str).str.strip().unique())
        vals = [v for v in vals if v]
        print(f"\n  rf -- уникальных филиалов: {len(vals)}")
        for v in vals:
            cnt = (df["rf"].astype(str).str.strip() == v).sum()
            print(f"    {v}: {cnt}")


# ============================================================================
# БЛОК 5. ЛОГИЧЕСКАЯ СОГЛАСОВАННОСТЬ
# ============================================================================

def check_logic(df):
    """
    Перекрёстные проверки между полями — ищем противоречия:
    - legal_form vs client_name: оргформа в поле должна совпадать с началом названия.
    - legal_form vs длина ИНН: ИП/КФХ -> 12 цифр, юрлица -> 10 цифр.
    - segment vs legal_form: ИП/КФХ в сегменте крупного/среднего бизнеса — подозрительно.
    """
    section("5. ЛОГИЧЕСКАЯ СОГЛАСОВАННОСТЬ")

    # --- Оргформа в поле legal_form должна совпадать с началом client_name ---
    # Например, legal_form="ООО", а client_name='ООО "Ромашка"'
    if "legal_form" in df.columns and "client_name" in df.columns:
        mismatches = 0
        examples = []
        for _, row in df.iterrows():
            lf = str(row.get("legal_form", "")).strip()
            name = str(row.get("client_name", "")).strip()
            if lf and name and not name.upper().startswith(lf.upper()):
                mismatches += 1
                if len(examples) < 5:
                    examples.append((lf, name))
        print(f"  legal_form vs client_name:")
        print(f"    Несовпадений: {mismatches}")
        if examples:
            for lf, name in examples:
                print(f"    Пример: legal_form='{lf}', client_name='{name}'")

    # --- Длина ИНН должна соответствовать оргформе ---
    # ИП и КФХ — это физлица, у них ИНН 12 цифр.
    # Юрлица (ООО, АО и др.) — ИНН 10 цифр.
    if "legal_form" in df.columns and "inn" in df.columns:
        print(f"\n  legal_form vs длина ИНН:")
        problems = 0
        examples = []
        for _, row in df.iterrows():
            lf = str(row.get("legal_form", "")).strip()
            inn = str(row.get("inn", "")).strip()
            if not inn or not lf:
                continue
            is_ip = lf in IP_LEGAL_FORMS
            inn_len = len(inn)
            if is_ip and inn_len != 12:
                problems += 1
                if len(examples) < 5:
                    examples.append((lf, inn, inn_len))
            elif not is_ip and inn_len != 10:
                problems += 1
                if len(examples) < 5:
                    examples.append((lf, inn, inn_len))
        print(f"    Несоответствий (ИП - 12 цифр, юрлицо - 10 цифр): {problems}")
        if examples:
            for lf, inn, inn_len in examples:
                print(f"    Пример: legal_form='{lf}', inn='{inn}' (длина {inn_len})")
        if problems == 0:
            print(f"    [OK] Все соответствуют")

    # --- ИП/КФХ в сегменте крупного или среднего бизнеса — подозрительно ---
    # Индивидуальный предприниматель в ДКБ/ДСБ — маловероятно, стоит проверить
    if "segment" in df.columns and "legal_form" in df.columns:
        print(f"\n  Сегмент vs оргформа (подозрительные сочетания):")
        suspicious = df[
            (df["legal_form"].isin(["ИП", "КФХ"])) &
            (df["segment"].isin(["ДКБ", "ДСБ"]))
        ]
        print(f"    ИП/КФХ в сегменте ДКБ/ДСБ: {len(suspicious)}")
        if len(suspicious) > 0:
            for _, row in suspicious.head(5).iterrows():
                print(f"    Пример: {row['client_name']} ({row['legal_form']}) -- сегмент {row['segment']}")


# ============================================================================
# БЛОК 6. СТАТИСТИЧЕСКИЙ ПРОФИЛЬ
# ============================================================================

def check_statistics(df):
    """
    Выводит общую статистику по данным — помогает увидеть аномалии:
    - Распределение по сегментам (с визуальной шкалой).
    - Распределение по типам мониторинга.
    - Топ-5 филиалов по количеству ФП.
    - Распределение дат по годам и месяцам; поиск аномальных всплесков
      (массовая загрузка в один день может означать техническую миграцию).
    - Топ-10 самых частых факторов проблемности.
    """
    section("6. СТАТИСТИЧЕСКИЙ ПРОФИЛЬ")

    # --- Распределение по сегментам бизнеса ---
    if "segment" in df.columns:
        print("  Распределение по сегментам:")
        dist = df["segment"].value_counts()
        for val, cnt in dist.items():
            pct = cnt / len(df) * 100
            bar = "#" * int(pct / 2)
            print(f"    {val:35s} {cnt:>6d} ({pct:5.1f}%) {bar}")

    # --- Распределение по типам мониторинга ---
    if "mon_type" in df.columns:
        print("\n  Распределение по типам мониторинга:")
        mon_vals = df["mon_type"].fillna("(пусто)").replace("", "(пусто)")
        dist = mon_vals.value_counts()
        for val, cnt in dist.items():
            pct = cnt / len(df) * 100
            print(f"    {val:35s} {cnt:>6d} ({pct:5.1f}%)")

    # --- Топ-5 региональных филиалов по количеству ФП ---
    if "rf" in df.columns:
        print(f"\n  Топ-5 филиалов по количеству ФП:")
        dist = df["rf"].value_counts().head(5)
        for val, cnt in dist.items():
            print(f"    {val}: {cnt}")

    # --- Анализ дат выявления ФП ---
    if "fp_start_date" in df.columns:
        # Распределение по годам — видны ли тренды роста/снижения
        print(f"\n  Распределение дат выявления ФП по годам:")
        dates_parsed = pd.to_datetime(df["fp_start_date"], format=DATE_FORMAT, errors="coerce")
        year_dist = dates_parsed.dt.year.value_counts().sort_index()
        for year, cnt in year_dist.items():
            if pd.notna(year):
                print(f"    {int(year)}: {cnt}")

        # Пиковый месяц — если резко выделяется, возможна массовая загрузка
        print(f"\n  Распределение по месяцам (все годы):")
        month_dist = dates_parsed.dt.to_period("M").value_counts().sort_index()
        max_month = month_dist.idxmax()
        max_val = month_dist.max()
        print(f"    Пиковый месяц: {max_month} ({max_val} ФП)")

        # Поиск дней с аномально большим числом ФП
        # Если в один день заведено >5 ФП, это может быть массовая миграция/загрузка
        day_dist = dates_parsed.value_counts()
        top_days = day_dist.head(5)
        if top_days.iloc[0] > 5:
            print(f"\n  [?] Даты с аномально большим числом ФП:")
            for date, cnt in top_days.items():
                if cnt > 3:
                    print(f"    {date.strftime(DATE_FORMAT)}: {cnt} ФП (возможна массовая загрузка?)")

    # --- Топ-10 самых частых факторов проблемности ---
    if "ref_book_fp_id" in df.columns:
        print(f"\n  Топ-10 факторов проблемности (ref_book_fp_id):")
        dist = df["ref_book_fp_id"].value_counts().head(10)
        for val, cnt in dist.items():
            pct = cnt / len(df) * 100
            print(f"    ФП #{val}: {cnt} ({pct:.1f}%)")


# ============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ============================================================================

def main():
    """Запускает все проверки последовательно."""
    print("=" * 70)
    print("  ПРОВЕРКА КАЧЕСТВА ДАННЫХ: ИСТОЧНИК H2O (Факторы проблемности)")
    print(f"  Файл: {FILE_PATH}")
    print(f"  Дата проверки: {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print("=" * 70)

    df = load_data()

    check_shape(df)         # 0. Размер таблицы и колонки
    check_nulls(df)         # 1. Пропуски
    check_duplicates(df)    # 2. Дубликаты
    check_formats(df)       # 3. Формат значений
    check_reference_values(df)  # 4. Справочные перечни
    check_logic(df)         # 5. Логическая согласованность
    check_statistics(df)    # 6. Статистика

    section("ПРОВЕРКА ЗАВЕРШЕНА")
    print("  Проанализируйте результаты выше и при необходимости")
    print("  скорректируйте допустимые перечни в начале скрипта.")


# ============================================================================
# ТОЧКА ВХОДА
# ============================================================================

if __name__ == "__main__":
    # Перехватываем весь вывод, чтобы одновременно показать в консоли
    # и сохранить в текстовый файл report_h20.txt
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
