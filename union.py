import pandas as pd

# Предполагаем, что df_union_pay уже загружен
# with imp:
#     df_union_pay = imp.fetch("""...""")

# ============================================
# 1) Количество строк
# ============================================
print("=" * 50)
print("1) КОЛИЧЕСТВО СТРОК")
print("=" * 50)
print(f"Всего строк: {len(df_union_pay)}")
print(f"Всего колонок: {len(df_union_pay.columns)}")

# ============================================
# 2) Максимальная и минимальная даты
# ============================================
print("\n" + "=" * 50)
print("2) ДИАПАЗОН ДАТ")
print("=" * 50)

date_cols = ['d_crd_issued', 'd_crd_expiry', 'd_crd_activated']
for col in date_cols:
    if col in df_union_pay.columns:
        # Преобразуем в datetime если нужно
        df_union_pay[col] = pd.to_datetime(df_union_pay[col], errors='coerce')
        min_date = df_union_pay[col].min()
        max_date = df_union_pay[col].max()
        print(f"{col}:")
        print(f"   MIN: {min_date}")
        print(f"   MAX: {max_date}")

# ============================================
# 3) Аномалии: дата окончания раньше даты выпуска
# ============================================
print("\n" + "=" * 50)
print("3) АНОМАЛИИ: дата окончания < дата выпуска")
print("=" * 50)

anomaly_expiry_before_issued = df_union_pay[
    df_union_pay['d_crd_expiry'] < df_union_pay['d_crd_issued']
]
print(f"Карт с датой окончания раньше даты выпуска: {len(anomaly_expiry_before_issued)}")
if len(anomaly_expiry_before_issued) > 0:
    print("\nПримеры аномалий:")
    print(anomaly_expiry_before_issued[['c_nact', 'd_crd_issued', 'd_crd_expiry']].head(10))

# Дополнительно: активация раньше выпуска
anomaly_activated_before_issued = df_union_pay[
    df_union_pay['d_crd_activated'] < df_union_pay['d_crd_issued']
]
print(f"\nКарт с датой активации раньше даты выпуска: {len(anomaly_activated_before_issued)}")

# ============================================
# 4) Неактивированные карты (d_crd_activated = NULL)
# ============================================
print("\n" + "=" * 50)
print("4) НЕАКТИВИРОВАННЫЕ КАРТЫ")
print("=" * 50)

not_activated = df_union_pay[df_union_pay['d_crd_activated'].isna()]
print(f"Карт без активации (NULL): {len(not_activated)}")
print(f"Доля неактивированных: {len(not_activated) / len(df_union_pay) * 100:.2f}%")

# ============================================
# 5) Дубликаты
# ============================================
print("\n" + "=" * 50)
print("5) ДУБЛИКАТЫ")
print("=" * 50)

# Полные дубликаты (все колонки одинаковые)
full_duplicates = df_union_pay.duplicated().sum()
print(f"Полных дубликатов (все колонки): {full_duplicates}")

# Дубликаты по ключевым полям
key_cols = ['c_nact', 'd_crd_issued', 'd_crd_expiry']
key_duplicates = df_union_pay.duplicated(subset=key_cols).sum()
print(f"Дубликатов по [{', '.join(key_cols)}]: {key_duplicates}")

# ============================================
# 6) Пропуски в данных
# ============================================
print("\n" + "=" * 50)
print("6) ПРОПУСКИ В ДАННЫХ")
print("=" * 50)

missing = df_union_pay.isnull().sum()
missing_pct = (df_union_pay.isnull().sum() / len(df_union_pay) * 100).round(2)

missing_df = pd.DataFrame({
    'Пропусков': missing,
    'Процент': missing_pct
})
# Показываем только колонки с пропусками или все
print(missing_df[missing_df['Пропусков'] > 0])
if missing_df['Пропусков'].sum() == 0:
    print("Пропусков нет!")

# ============================================
# 7) Несколько карт на один счёт
# ============================================
print("\n" + "=" * 50)
print("7) НЕСКОЛЬКО КАРТ НА ОДИН СЧЁТ")
print("=" * 50)

cards_per_account = df_union_pay.groupby('c_nact').size()
accounts_with_multiple_cards = cards_per_account[cards_per_account > 1]

print(f"Всего уникальных счетов: {len(cards_per_account)}")
print(f"Счетов с несколькими картами: {len(accounts_with_multiple_cards)}")

if len(accounts_with_multiple_cards) > 0:
    print(f"\nРаспределение количества карт на счёт:")
    print(cards_per_account.value_counts().sort_index())
    print(f"\nТоп-5 счетов с наибольшим количеством карт:")
    print(accounts_with_multiple_cards.sort_values(ascending=False).head(5))

# ============================================
# СВОДКА
# ============================================
print("\n" + "=" * 50)
print("ИТОГОВАЯ СВОДКА")
print("=" * 50)
print(f"Всего записей:                    {len(df_union_pay)}")
print(f"Уникальных счетов:                {df_union_pay['c_nact'].nunique()}")
print(f"Неактивированных карт:            {len(not_activated)}")
print(f"Аномалий (expiry < issued):       {len(anomaly_expiry_before_issued)}")
print(f"Полных дубликатов:                {full_duplicates}")
print(f"Счетов с несколькими картами:     {len(accounts_with_multiple_cards)}")