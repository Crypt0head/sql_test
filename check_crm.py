"""
Скрипт проверки качества данных из источника ЕЦП.CRM (мастер-система ФП/СФП).

ВАЖНО: скрипт оптимизирован для работы с большими объёмами данных (1.5 млн строк).
Все проверки выполнены через векторизованные операции pandas, без iterrows().

Выполняет 7 блоков проверок:
  0. Общая информация — размер таблицы, список колонок.
  1. Пропуски — NULL/пустые значения по каждому полю.
  2. Дубликаты — полные дубли, дубли по ROW_ID, по ИНН.
  3. Формат и валидность — ИНН, КПП, ОГРН, даты, суммы, флаги.
  4. Справочные значения — статусы, типы, валюты, источники.
  5. Логическая согласованность — перекрёстные проверки дат и статусов.
  6. Статистический профиль — распределения по ключевым полям.

Использование:
  python check_crm.py
"""

import pandas as pd
import numpy as np
import sys
import io
import os
from datetime import datetime

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)

# ============================================================================
# НАСТРОЙКИ
# ============================================================================

FILE_PATH = os.path.join(_PROJECT_ROOT, "sources", "data_crm.csv")
REPORT_PATH = os.path.join(_PROJECT_ROOT, "report_crm.txt")
ENCODING = "utf-8-sig"
SEP = ";"

# Известные значения справочных полей (дополните по мере уточнения)
KNOWN_SOURCES = {"H2O", "CRM", "АБС", "Diasoft", "ППРБ"}
KNOWN_STATUSES_FP = {"Открыт", "Закрыт", "В работе", "На согласовании"}
KNOWN_TYPES_FP = {"ФП", "СФП"}
KNOWN_CURRENCIES = {"RUB", "USD", "EUR", "CNY", "GBP", "CHF"}
KNOWN_YN_VALUES = {"Y", "N"}
KNOWN_COMPANY_STATUSES = {"Действующая", "Не действующая", "В стадии ликвидации", "Банкрот"}

DATE_FORMAT = "%d.%m.%Y"
DATE_MIN = datetime(2018, 1, 1)
DATE_MAX = datetime(2026, 12, 31)

# Колонки с датами — для массовой проверки формата
DATE_COLUMNS = [
    "DATE_END_FP_SFP", "END_DATE_SCR_FCT", "END_DATE_SCR_PLAN",
    "END_EVENT_DATE_FACT", "FIRST_END_DATE_EVENT", "IDENTIFICATION_DATE",
    "NEW_PLAN_END_DATE_EVT", "AGREEMENT_OPEN_DT", "AGREEMENT_CLOSE_DT",
]

# Обязательные поля — не должны быть пустыми
REQUIRED_FIELDS = ["ROW_ID", "X_INN", "IDENTIFICATION_DATE", "TYPE_FP", "VAL_1"]


# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================

def section(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def is_empty(series):
    """Считает количество пустых значений (NaN + пустые строки) векторизованно."""
    return series.isna() | (series.astype(str).str.strip() == "")


def parse_dates(series):
    """Парсит колонку дат векторизованно, возвращает pd.Series[datetime64]."""
    return pd.to_datetime(series, format=DATE_FORMAT, errors="coerce")


def load_data():
    """
    Загрузка CSV. Все колонки как строки (dtype=str) — чтобы не потерять
    ведущие нули в ИНН/КПП/ОГРН.
    """
    df = pd.read_csv(FILE_PATH, sep=SEP, encoding=ENCODING, dtype=str, low_memory=False)
    df.columns = df.columns.str.strip()
    print(f"Загружено строк: {len(df):,}, колонок: {len(df.columns)}")
    print(f"Колонки: {list(df.columns)}")
    return df


# ============================================================================
# БЛОК 0. ОБЩАЯ ИНФОРМАЦИЯ
# ============================================================================

def check_shape(df):
    """Размер таблицы и перечень колонок."""
    section("0. ОБЩАЯ ИНФОРМАЦИЯ")
    print(f"Строк: {len(df):,}")
    print(f"Колонок: {len(df.columns)}")
    print(f"Список колонок: {list(df.columns)}")


# ============================================================================
# БЛОК 1. ПРОПУСКИ
# ============================================================================

def check_nulls(df):
    """
    Подсчёт пропусков по каждой колонке.
    Отдельно выделяет критичные поля (ROW_ID, X_INN, IDENTIFICATION_DATE и др.).
    """
    section("1. ПРОПУСКИ (NULL / пустые значения)")

    for col in df.columns:
        nulls = is_empty(df[col]).sum()
        pct = nulls / len(df) * 100
        status = "[!]" if pct > 0 else "[OK]"
        if pct > 0:
            print(f"  {status} {col}: {nulls:,} пропусков ({pct:.1f}%)")
        else:
            print(f"  {status} {col}: нет пропусков")

    for col in REQUIRED_FIELDS:
        if col not in df.columns:
            continue
        nulls = is_empty(df[col]).sum()
        if nulls > 0:
            print(f"\n  [!!!] КРИТИЧНО: '{col}' -- {nulls:,} пропусков. Это обязательное поле!")


# ============================================================================
# БЛОК 2. ДУБЛИКАТЫ
# ============================================================================

def check_duplicates(df):
    """
    - Полные дубли строк.
    - Дубли по ROW_ID (должен быть уникальным ID факторапроблемности).
    - Анализ повторяющихся ИНН (у одного клиента может быть много ФП).
    """
    section("2. ДУБЛИКАТЫ")

    # Полные дубликаты
    full_dupes = df.duplicated().sum()
    print(f"  Полные дубликаты строк: {full_dupes:,}")
    if full_dupes > 0:
        print(f"  [!] Вероятна ошибка загрузки/ETL")

    # ROW_ID — уникальный идентификатор ФП
    if "ROW_ID" in df.columns:
        clean = df[~is_empty(df["ROW_ID"])]
        dupes = clean["ROW_ID"].duplicated().sum()
        print(f"  Дубликаты ROW_ID: {dupes:,}")
        if dupes > 0:
            print(f"  [!] ROW_ID должен быть уникальным")
            examples = clean[clean["ROW_ID"].duplicated(keep=False)]["ROW_ID"].unique()[:5]
            print(f"      Примеры: {examples.tolist()}")

    # ИНН — повторы ожидаемы (у клиента несколько ФП)
    if "X_INN" in df.columns:
        inn_counts = df["X_INN"].value_counts()
        total = df["X_INN"].nunique()
        repeats = (inn_counts > 1).sum()
        print(f"\n  Уникальных ИНН: {total:,}")
        print(f"  ИНН с несколькими записями: {repeats:,}")
        if repeats > 0:
            print(f"  (Допустимо -- у клиента может быть несколько ФП/СФП и сделок)")
            print(f"\n  Топ-5 ИНН по количеству записей:")
            for inn_val, cnt in inn_counts.head(5).items():
                print(f"    ИНН {inn_val}: {cnt} записей")


# ============================================================================
# БЛОК 3. ФОРМАТ И ВАЛИДНОСТЬ ЗНАЧЕНИЙ
# ============================================================================

def check_formats(df):
    """
    Векторизованные проверки форматов:
    - X_INN: цифры, длина 10/12
    - X_KPP: цифры, длина 9
    - X_OGRN: цифры, длина 13/15
    - Все даты: формат, диапазон
    - APPROVED_SUM: числовое, >0
    - DEFOLT, AGR_OPEN_DT_FLG: Y/N
    """
    section("3. ФОРМАТ И ВАЛИДНОСТЬ ЗНАЧЕНИЙ")

    # --- ИНН ---
    if "X_INN" in df.columns:
        col = df["X_INN"].dropna().astype(str).str.strip()
        col = col[col != ""]
        bad_chars = (~col.str.match(r"^\d+$")).sum()
        bad_len = (~col.str.len().isin([10, 12])).sum()
        print(f"  X_INN:")
        print(f"    Не только цифры: {bad_chars:,}")
        print(f"    Длина не 10 и не 12: {bad_len:,}")
        print(f"    Распределение по длине: {dict(col.str.len().value_counts().sort_index())}")

    # --- КПП ---
    if "X_KPP" in df.columns:
        col = df["X_KPP"].dropna().astype(str).str.strip()
        col = col[col != ""]
        bad_chars = (~col.str.match(r"^\d+$")).sum()
        bad_len = (col.str.len() != 9).sum()
        print(f"\n  X_KPP:")
        print(f"    Не только цифры: {bad_chars:,}")
        print(f"    Длина не 9: {bad_len:,}")

    # --- ОГРН ---
    if "X_OGRN" in df.columns:
        col = df["X_OGRN"].dropna().astype(str).str.strip()
        col = col[col != ""]
        bad_chars = (~col.str.match(r"^\d+$")).sum()
        bad_len = (~col.str.len().isin([13, 15])).sum()
        print(f"\n  X_OGRN:")
        print(f"    Не только цифры: {bad_chars:,}")
        print(f"    Длина не 13 и не 15: {bad_len:,}")
        print(f"    Распределение по длине: {dict(col.str.len().value_counts().sort_index())}")

    # --- Все даты ---
    print(f"\n  Проверка дат (формат, диапазон {DATE_MIN.year}-{DATE_MAX.year}):")
    for dcol in DATE_COLUMNS:
        if dcol not in df.columns:
            continue
        raw = df[dcol].astype(str).str.strip()
        filled = raw[(raw != "") & (raw != "nan") & (raw != "None")]
        if len(filled) == 0:
            print(f"    {dcol}: все пусто")
            continue
        parsed = pd.to_datetime(filled, format=DATE_FORMAT, errors="coerce")
        bad_fmt = parsed.isna().sum()
        valid = parsed.dropna()
        before = (valid < DATE_MIN).sum()
        after = (valid > DATE_MAX).sum()
        status = "[OK]" if (bad_fmt == 0 and before == 0 and after == 0) else "[!]"
        print(f"    {status} {dcol}: заполнено {len(filled):,}, "
              f"некорр. формат {bad_fmt:,}, "
              f"ранее {DATE_MIN.year} -- {before:,}, "
              f"позднее {DATE_MAX.year} -- {after:,}")

    # --- APPROVED_SUM (сумма по договору) ---
    if "APPROVED_SUM" in df.columns:
        print(f"\n  APPROVED_SUM (сумма по договору):")
        col = df["APPROVED_SUM"].astype(str).str.strip()
        col = col[(col != "") & (col != "nan")]
        numeric = pd.to_numeric(col, errors="coerce")
        non_numeric = numeric.isna().sum()
        negatives = (numeric < 0).sum()
        zeros = (numeric == 0).sum()
        print(f"    Нечисловые: {non_numeric:,}")
        print(f"    Отрицательные: {negatives:,}")
        print(f"    Нулевые: {zeros:,}")
        if numeric.dropna().shape[0] > 0:
            print(f"    Диапазон: {numeric.min():,.2f} -- {numeric.max():,.2f}")

    # --- Флаги Y/N ---
    yn_cols = ["DEFOLT", "AGR_OPEN_DT_FLG"]
    for ycol in yn_cols:
        if ycol not in df.columns:
            continue
        col = df[ycol].dropna().astype(str).str.strip()
        col = col[col != ""]
        vals = set(col.unique())
        unexpected = vals - KNOWN_YN_VALUES
        print(f"\n  {ycol} (Y/N):")
        print(f"    Уникальные: {sorted(vals)}")
        if unexpected:
            print(f"    [!] Неожиданные значения: {sorted(unexpected)}")
        else:
            print(f"    [OK]")


# ============================================================================
# БЛОК 4. СПРАВОЧНЫЕ ЗНАЧЕНИЯ
# ============================================================================

def check_reference_values(df):
    """
    Сверка значений справочных полей с допустимыми перечнями.
    Для каждого поля выводит уникальные значения и подсвечивает неизвестные.
    """
    section("4. СПРАВОЧНЫЕ ЗНАЧЕНИЯ (допустимые перечни)")

    checks = [
        ("VAL", "система-источник", KNOWN_SOURCES),
        ("VAL_1", "статус ФП/СФП", KNOWN_STATUSES_FP),
        ("TYPE_FP", "тип (ФП/СФП)", KNOWN_TYPES_FP),
        ("CURCY_CD", "валюта", KNOWN_CURRENCIES),
    ]

    for col, label, known in checks:
        if col not in df.columns:
            continue
        vals = set(df[col].dropna().astype(str).str.strip().unique()) - {""}
        unknown = vals - known
        print(f"  {col} ({label}) -- уникальные: {sorted(vals)}")
        if unknown:
            print(f"  [?] Неизвестные значения: {sorted(unknown)}")
        else:
            print(f"  [OK] Все из известного перечня")
        dist = df[col].value_counts().head(10)
        for val, cnt in dist.items():
            print(f"    {val}: {cnt:,} ({cnt/len(df)*100:.1f}%)")
        print()

    # Статусы компании (внешний и внутренний) — выводим для ручного анализа
    for col, label in [("STATUS", "статус компании (СПАРК)"), ("VAL_3", "статус компании (внутр.)")]:
        if col not in df.columns:
            continue
        vals = set(df[col].dropna().astype(str).str.strip().unique()) - {""}
        print(f"  {col} ({label}) -- уникальные: {sorted(vals)}")
        dist = df[col].value_counts()
        for val, cnt in dist.items():
            print(f"    {val}: {cnt:,} ({cnt/len(df)*100:.1f}%)")
        print()

    # VAL_2 (стадия сделки) — выводим для ручного анализа
    if "VAL_2" in df.columns:
        vals = sorted(set(df["VAL_2"].dropna().astype(str).str.strip().unique()) - {""})
        print(f"  VAL_2 (стадия сделки) -- уникальные: {vals}")
        dist = df["VAL_2"].value_counts()
        for val, cnt in dist.items():
            print(f"    {val}: {cnt:,} ({cnt/len(df)*100:.1f}%)")


# ============================================================================
# БЛОК 5. ЛОГИЧЕСКАЯ СОГЛАСОВАННОСТЬ
# ============================================================================

def check_logic(df):
    """
    Перекрёстные проверки (все векторизованные):
    - Статус 'Закрыт' -> DATE_END_FP_SFP заполнена
    - Статус 'Открыт' -> DATE_END_FP_SFP пуста
    - IDENTIFICATION_DATE <= DATE_END_FP_SFP
    - AGREEMENT_OPEN_DT <= AGREEMENT_CLOSE_DT
    - END_DATE_SCR_PLAN >= IDENTIFICATION_DATE
    - FIRST_END_DATE_EVENT <= END_EVENT_DATE_FACT (если оба заполнены)
    - DEFOLT=Y при STATUS='Действующая' — подозрительно
    - APPROVED=Y и DISABLED=Y одновременно — противоречие
    """
    section("5. ЛОГИЧЕСКАЯ СОГЛАСОВАННОСТЬ")

    # --- Статус 'Закрыт', но нет даты снятия ---
    if "VAL_1" in df.columns and "DATE_END_FP_SFP" in df.columns:
        closed = df[df["VAL_1"].astype(str).str.strip() == "Закрыт"]
        no_end = is_empty(closed["DATE_END_FP_SFP"]).sum()
        print(f"  Статус 'Закрыт', но DATE_END_FP_SFP пуста: {no_end:,}")
        if no_end > 0:
            print(f"    [?] Закрытый ФП должен иметь дату снятия с контроля")
        else:
            print(f"    [OK]")

    # --- Статус 'Открыт', но дата снятия заполнена ---
    if "VAL_1" in df.columns and "DATE_END_FP_SFP" in df.columns:
        opened = df[df["VAL_1"].astype(str).str.strip() == "Открыт"]
        has_end = (~is_empty(opened["DATE_END_FP_SFP"])).sum()
        print(f"\n  Статус 'Открыт', но DATE_END_FP_SFP заполнена: {has_end:,}")
        if has_end > 0:
            print(f"    [?] Открытый ФП не должен иметь дату снятия")
        else:
            print(f"    [OK]")

    # --- IDENTIFICATION_DATE <= DATE_END_FP_SFP ---
    if "IDENTIFICATION_DATE" in df.columns and "DATE_END_FP_SFP" in df.columns:
        id_dt = parse_dates(df["IDENTIFICATION_DATE"])
        end_dt = parse_dates(df["DATE_END_FP_SFP"])
        mask = id_dt.notna() & end_dt.notna() & (end_dt < id_dt)
        cnt = mask.sum()
        print(f"\n  DATE_END_FP_SFP ранее IDENTIFICATION_DATE: {cnt:,}")
        if cnt > 0:
            print(f"    [!] Дата снятия не может быть раньше даты выявления")
            examples = df[mask][["X_INN", "IDENTIFICATION_DATE", "DATE_END_FP_SFP"]].head(3)
            for _, r in examples.iterrows():
                print(f"    ИНН {r['X_INN']}: выявлено {r['IDENTIFICATION_DATE']}, снято {r['DATE_END_FP_SFP']}")
        else:
            print(f"    [OK]")

    # --- AGREEMENT_OPEN_DT <= AGREEMENT_CLOSE_DT ---
    if "AGREEMENT_OPEN_DT" in df.columns and "AGREEMENT_CLOSE_DT" in df.columns:
        open_dt = parse_dates(df["AGREEMENT_OPEN_DT"])
        close_dt = parse_dates(df["AGREEMENT_CLOSE_DT"])
        mask = open_dt.notna() & close_dt.notna() & (close_dt < open_dt)
        cnt = mask.sum()
        print(f"\n  AGREEMENT_CLOSE_DT ранее AGREEMENT_OPEN_DT: {cnt:,}")
        if cnt > 0:
            print(f"    [!] Дата закрытия договора ранее даты открытия")
        else:
            print(f"    [OK]")

    # --- Плановая дата окончания сценария >= даты выявления ---
    if "END_DATE_SCR_PLAN" in df.columns and "IDENTIFICATION_DATE" in df.columns:
        plan_dt = parse_dates(df["END_DATE_SCR_PLAN"])
        id_dt = parse_dates(df["IDENTIFICATION_DATE"])
        mask = plan_dt.notna() & id_dt.notna() & (plan_dt < id_dt)
        cnt = mask.sum()
        print(f"\n  END_DATE_SCR_PLAN ранее IDENTIFICATION_DATE: {cnt:,}")
        if cnt > 0:
            print(f"    [?] План окончания сценария раньше выявления ФП")
        else:
            print(f"    [OK]")

    # --- APPROVED=Y и DISABLED=Y одновременно ---
    if "APPROVED" in df.columns and "DISABLED" in df.columns:
        both = (
            (df["APPROVED"].astype(str).str.strip() == "Y") &
            (df["DISABLED"].astype(str).str.strip() == "Y")
        ).sum()
        print(f"\n  APPROVED=Y и DISABLED=Y одновременно: {both:,}")
        if both > 0:
            print(f"    [!] Противоречие: ФП не может быть одновременно подтверждён и отклонён")
        else:
            print(f"    [OK]")

    # --- DEFOLT=Y при STATUS='Действующая' ---
    if "DEFOLT" in df.columns and "STATUS" in df.columns:
        suspicious = (
            (df["DEFOLT"].astype(str).str.strip() == "Y") &
            (df["STATUS"].astype(str).str.strip() == "Действующая")
        ).sum()
        print(f"\n  DEFOLT=Y при STATUS='Действующая' (СПАРК): {suspicious:,}")
        if suspicious > 0:
            print(f"    [i] Клиент в дефолте, но по СПАРК компания действующая -- не ошибка, но стоит учесть")
        else:
            print(f"    [OK]")

    # --- STATUS (внешний) vs VAL_3 (внутренний) ---
    if "STATUS" in df.columns and "VAL_3" in df.columns:
        mismatch = (
            (df["STATUS"].astype(str).str.strip() == "Действующая") &
            (df["VAL_3"].astype(str).str.strip() == "Не действующая")
        ).sum()
        mismatch2 = (
            (df["STATUS"].astype(str).str.strip() == "Не действующая") &
            (df["VAL_3"].astype(str).str.strip() == "Действующая")
        ).sum()
        total_mm = mismatch + mismatch2
        print(f"\n  Расхождение STATUS (СПАРК) и VAL_3 (внутр.): {total_mm:,}")
        if total_mm > 0:
            print(f"    [?] Внешний и внутренний статусы компании противоречат друг другу")
            print(f"    СПАРК=Действующая, Внутр=Не действующая: {mismatch:,}")
            print(f"    СПАРК=Не действующая, Внутр=Действующая: {mismatch2:,}")
        else:
            print(f"    [OK]")


# ============================================================================
# БЛОК 6. СТАТИСТИЧЕСКИЙ ПРОФИЛЬ
# ============================================================================

def check_statistics(df):
    """Ключевые распределения для выявления аномалий."""
    section("6. СТАТИСТИЧЕСКИЙ ПРОФИЛЬ")

    # --- По типу ФП/СФП ---
    if "TYPE_FP" in df.columns:
        print("  Распределение по TYPE_FP:")
        for val, cnt in df["TYPE_FP"].value_counts().items():
            print(f"    {val}: {cnt:,} ({cnt/len(df)*100:.1f}%)")

    # --- По статусу ---
    if "VAL_1" in df.columns:
        print(f"\n  Распределение по статусу (VAL_1):")
        for val, cnt in df["VAL_1"].value_counts().items():
            pct = cnt / len(df) * 100
            bar = "#" * int(pct / 2)
            print(f"    {val:25s} {cnt:>8,} ({pct:5.1f}%) {bar}")

    # --- По источнику ---
    if "VAL" in df.columns:
        print(f"\n  Распределение по источнику (VAL):")
        for val, cnt in df["VAL"].value_counts().items():
            print(f"    {val}: {cnt:,} ({cnt/len(df)*100:.1f}%)")

    # --- Дефолт ---
    if "DEFOLT" in df.columns:
        print(f"\n  DEFOLT (Y/N):")
        for val, cnt in df["DEFOLT"].value_counts().items():
            print(f"    {val}: {cnt:,} ({cnt/len(df)*100:.1f}%)")

    # --- По годам выявления ---
    if "IDENTIFICATION_DATE" in df.columns:
        print(f"\n  Распределение по годам выявления:")
        dates = parse_dates(df["IDENTIFICATION_DATE"])
        year_dist = dates.dt.year.value_counts().sort_index()
        for year, cnt in year_dist.items():
            if pd.notna(year):
                print(f"    {int(year)}: {cnt:,}")

    # --- По валюте ---
    if "CURCY_CD" in df.columns:
        print(f"\n  Распределение по валюте (CURCY_CD):")
        for val, cnt in df["CURCY_CD"].value_counts().items():
            print(f"    {val}: {cnt:,} ({cnt/len(df)*100:.1f}%)")

    # --- По продукту ---
    if "NAME" in df.columns:
        print(f"\n  Топ-10 продуктов (NAME):")
        for val, cnt in df["NAME"].value_counts().head(10).items():
            print(f"    {val}: {cnt:,} ({cnt/len(df)*100:.1f}%)")

    # --- Суммы ---
    if "APPROVED_SUM" in df.columns:
        sums = pd.to_numeric(df["APPROVED_SUM"], errors="coerce").dropna()
        if len(sums) > 0:
            print(f"\n  APPROVED_SUM (сумма по договору):")
            print(f"    Среднее: {sums.mean():,.2f}")
            print(f"    Медиана: {sums.median():,.2f}")
            print(f"    Мин: {sums.min():,.2f}")
            print(f"    Макс: {sums.max():,.2f}")

    # --- По сценарию ---
    if "SCRIPT" in df.columns:
        print(f"\n  Распределение по сценарию (SCRIPT):")
        vals = df["SCRIPT"].fillna("(пусто)").replace("", "(пусто)")
        for val, cnt in vals.value_counts().items():
            print(f"    {val}: {cnt:,} ({cnt/len(df)*100:.1f}%)")


# ============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ============================================================================

def main():
    print("=" * 70)
    print("  ПРОВЕРКА КАЧЕСТВА ДАННЫХ: ИСТОЧНИК ЕЦП.CRM")
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
