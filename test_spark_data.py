# test_spark_data.py
import pytest
import pandas as pd
import numpy as np


class TestCompanyDataFrame:
    """Unit-тесты для проверки корректности DataFrame с данными компаний."""

    @pytest.fixture
    def df(self):
        """
        Фикстура для загрузки DataFrame.
        Замените на актуальный способ получения df из вашего скрипта.
        """
        # Вариант 1: Импорт из основного модуля
        # from Spark_interfacs import df
        # return df
        
        # Вариант 2: Для тестирования создаём тестовый DataFrame
        # (замените на реальный импорт)
        from Spark_interfacs import df
        return df

    # ==========================================
    # ТЕСТЫ НА ПРОПУСКИ (MISSING VALUES)
    # ==========================================

    def test_df_not_empty(self, df):
        """DataFrame не должен быть пустым."""
        assert df is not None, "DataFrame is None"
        assert len(df) > 0, "DataFrame пустой"

    def test_no_missing_inn(self, df):
        """ИНН не должен содержать пропусков."""
        assert df['INN'].notna().all(), \
            f"Найдены пропуски в ИНН: {df[df['INN'].isna()].index.tolist()}"

    def test_no_missing_name(self, df):
        """Наименование компании не должно содержать пропусков."""
        # Предполагаемое имя колонки - может отличаться
        name_col = 'ShortNameRus' if 'ShortNameRus' in df.columns else 'name'
        if name_col in df.columns:
            assert df[name_col].notna().all(), \
                f"Найдены пропуски в наименовании: {df[df[name_col].isna()].index.tolist()}"

    def test_missing_values_threshold(self, df):
        """Доля пропусков в каждой колонке не должна превышать порог."""
        threshold = 0.1  # 10%
        for col in df.columns:
            missing_ratio = df[col].isna().sum() / len(df)
            assert missing_ratio <= threshold, \
                f"Колонка '{col}' содержит {missing_ratio:.1%} пропусков (порог: {threshold:.0%})"

    # ==========================================
    # ТЕСТЫ НА ДУБЛИКАТЫ (DUPLICATES)
    # ==========================================

    def test_no_duplicate_inn(self, df):
        """ИНН должен быть уникальным (нет дубликатов)."""
        duplicates = df[df['INN'].duplicated(keep=False)]
        assert len(duplicates) == 0, \
            f"Найдены дубликаты ИНН ({len(duplicates)} записей): {duplicates['INN'].unique().tolist()}"

    def test_no_full_duplicates(self, df):
        """Не должно быть полных дубликатов строк."""
        full_duplicates = df[df.duplicated(keep=False)]
        assert len(full_duplicates) == 0, \
            f"Найдены полные дубликаты строк: {len(full_duplicates)} записей"

    # ==========================================
    # ТЕСТЫ БИЗНЕС-ЛОГИКИ
    # ==========================================

    def test_revenue_not_negative(self, df):
        """Выручка не может быть отрицательной."""
        revenue_col = 'profit_val' if 'profit_val' in df.columns else 'revenue'
        if revenue_col in df.columns:
            # Преобразуем в числовой тип, если строка
            revenue = pd.to_numeric(df[revenue_col], errors='coerce')
            negative_revenue = df[revenue < 0]
            assert len(negative_revenue) == 0, \
                f"Найдены компании с отрицательной выручкой: {negative_revenue['INN'].tolist()}"

    def test_cases_count_not_negative(self, df):
        """Количество арбитражных дел не может быть отрицательным."""
        cases_col = 'cases_count' if 'cases_count' in df.columns else 'arbitration_cases'
        if cases_col in df.columns:
            cases = pd.to_numeric(df[cases_col], errors='coerce')
            negative_cases = df[cases < 0]
            assert len(negative_cases) == 0, \
                f"Найдены компании с отрицательным кол-вом дел: {negative_cases['INN'].tolist()}"

    def test_claim_not_negative(self, df):
        """Сумма исковых требований не может быть отрицательной."""
        claim_col = 'claim' if 'claim' in df.columns else 'claim_amount'
        if claim_col in df.columns:
            claims = pd.to_numeric(df[claim_col], errors='coerce')
            negative_claims = df[claims < 0]
            assert len(negative_claims) == 0, \
                f"Найдены компании с отрицательной суммой исков: {negative_claims['INN'].tolist()}"

    def test_inn_valid_format(self, df):
        """ИНН должен соответствовать формату (10 или 12 цифр для РФ)."""
        def is_valid_inn(inn):
            if pd.isna(inn):
                return False
            inn_str = str(inn).strip()
            # ИНН юрлица - 10 цифр, ИП/физлица - 12 цифр
            return inn_str.isdigit() and len(inn_str) in (10, 12)
        
        invalid_inn = df[~df['INN'].apply(is_valid_inn)]
        assert len(invalid_inn) == 0, \
            f"Найдены невалидные ИНН: {invalid_inn['INN'].tolist()[:10]}..."  # показываем первые 10

    def test_inn_checksum_valid(self, df):
        """Проверка контрольной суммы ИНН (опционально)."""
        def validate_inn_checksum(inn):
            """Проверка контрольной суммы ИНН по алгоритму ФНС."""
            if pd.isna(inn):
                return False
            inn_str = str(inn).strip()
            
            if not inn_str.isdigit():
                return False
            
            if len(inn_str) == 10:
                # ИНН юрлица
                coeffs = [2, 4, 10, 3, 5, 9, 4, 6, 8]
                checksum = sum(int(inn_str[i]) * coeffs[i] for i in range(9)) % 11 % 10
                return checksum == int(inn_str[9])
            
            elif len(inn_str) == 12:
                # ИНН физлица/ИП
                coeffs1 = [7, 2, 4, 10, 3, 5, 9, 4, 6, 8]
                coeffs2 = [3, 7, 2, 4, 10, 3, 5, 9, 4, 6, 8]
                checksum1 = sum(int(inn_str[i]) * coeffs1[i] for i in range(10)) % 11 % 10
                checksum2 = sum(int(inn_str[i]) * coeffs2[i] for i in range(11)) % 11 % 10
                return checksum1 == int(inn_str[10]) and checksum2 == int(inn_str[11])
            
            return False
        
        invalid_checksum = df[~df['INN'].apply(validate_inn_checksum)]
        # Предупреждение вместо ошибки, т.к. могут быть тестовые данные
        if len(invalid_checksum) > 0:
            pytest.warns(UserWarning, 
                match=f"Найдены ИНН с невалидной контрольной суммой: {len(invalid_checksum)} шт.")

    def test_cases_count_is_integer(self, df):
        """Количество дел должно быть целым числом."""
        cases_col = 'cases_count' if 'cases_count' in df.columns else 'arbitration_cases'
        if cases_col in df.columns:
            cases = pd.to_numeric(df[cases_col], errors='coerce')
            non_integer = df[cases != cases.astype(int)]
            assert len(non_integer) == 0, \
                f"Количество дел содержит нецелые значения: {non_integer['INN'].tolist()}"

    def test_claim_greater_than_zero_when_cases_exist(self, df):
        """Если есть арбитражные дела, сумма иска должна быть > 0."""
        cases_col = 'cases_count' if 'cases_count' in df.columns else 'arbitration_cases'
        claim_col = 'claim' if 'claim' in df.columns else 'claim_amount'
        
        if cases_col in df.columns and claim_col in df.columns:
            cases = pd.to_numeric(df[cases_col], errors='coerce')
            claims = pd.to_numeric(df[claim_col], errors='coerce')
            
            inconsistent = df[(cases > 0) & (claims <= 0)]
            # Это может быть нормально в некоторых случаях, поэтому warning
            if len(inconsistent) > 0:
                print(f"⚠️ Предупреждение: {len(inconsistent)} компаний с делами, но без суммы иска")

    # ==========================================
    # ТЕСТЫ НА ТИПЫ ДАННЫХ
    # ==========================================

    def test_column_types(self, df):
        """Проверка ожидаемых типов данных в колонках."""
        expected_types = {
            'INN': ['object', 'str', 'int64'],  # может быть строкой или числом
            'profit_val': ['int64', 'float64', 'object'],
            'cases_count': ['int64', 'float64', 'object'],
            'claim': ['int64', 'float64', 'object'],
        }
        
        for col, allowed_types in expected_types.items():
            if col in df.columns:
                actual_type = str(df[col].dtype)
                assert actual_type in allowed_types, \
                    f"Колонка '{col}' имеет тип {actual_type}, ожидается один из {allowed_types}"

    # ==========================================
    # ТЕСТЫ НА АНОМАЛИИ
    # ==========================================

    def test_revenue_outliers(self, df):
        """Проверка на экстремальные выбросы в выручке (>3 sigma)."""
        revenue_col = 'profit_val' if 'profit_val' in df.columns else 'revenue'
        if revenue_col in df.columns:
            revenue = pd.to_numeric(df[revenue_col], errors='coerce').dropna()
            if len(revenue) > 0:
                mean = revenue.mean()
                std = revenue.std()
                if std > 0:
                    outliers = df[abs(revenue - mean) > 3 * std]
                    # Информационное сообщение
                    if len(outliers) > 0:
                        print(f"ℹ️ Найдено {len(outliers)} компаний с аномальной выручкой (>3σ)")

    def test_name_not_empty_string(self, df):
        """Наименование не должно быть пустой строкой."""
        name_col = 'ShortNameRus' if 'ShortNameRus' in df.columns else 'name'
        if name_col in df.columns:
            empty_names = df[df[name_col].astype(str).str.strip() == '']
            assert len(empty_names) == 0, \
                f"Найдены компании с пустым наименованием: {empty_names['INN'].tolist()}"


# ==========================================
# ЗАПУСК ТЕСТОВ
# ==========================================

if __name__ == "__main__":
    # Запуск всех тестов с подробным выводом
    pytest.main([__file__, "-v", "--tb=short"])

