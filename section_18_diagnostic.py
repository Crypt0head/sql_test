# Секция 18: диагностика commission_monthly и retl_cnt
# Скопировать в новую ячейку datamart_month_lake_agr.ipynb (или любого ноутбука,
# где уже есть переменные both_df, lake_check_df, excel_check_df, diag).

if not excel_check_df.empty and not both_df.empty:
    diag = both_df.copy()
    diag['cm_lake'] = pd.to_numeric(diag.get('commission_monthly_lake'), errors='coerce')
    diag['cm_excel'] = pd.to_numeric(diag.get('commission_monthly_excel'), errors='coerce')
    diag['rc_lake'] = pd.to_numeric(diag.get('retl_cnt_lake'), errors='coerce')
    diag['rc_excel'] = pd.to_numeric(diag.get('retl_cnt_excel'), errors='coerce')

    diag['delta_cm'] = diag['cm_lake'].fillna(0) - diag['cm_excel'].fillna(0)
    diag['delta_rc'] = diag['rc_lake'].fillna(0) - diag['rc_excel'].fillna(0)

    print('=' * 70)
    print('ДИАГНОСТИКА: commission_monthly')
    print('=' * 70)
    print(f'\nВсего договоров на пересечении: {len(diag):,}')
    print(f'  cm_lake = NULL:                   {int(diag["cm_lake"].isna().sum()):,}')
    print(f'  cm_lake = 0 (но не null):         {int((diag["cm_lake"] == 0).sum()):,}')
    print(f'  cm_lake > 0 и cm_excel > 0:       {int(((diag["cm_lake"] > 0) & (diag["cm_excel"] > 0)).sum()):,}')
    print(f'  cm_excel = NULL/0:                {int((diag["cm_excel"].fillna(0) == 0).sum()):,}')
    print(f'  cm_excel > 0 НО cm_lake NULL/0:   {int(((diag["cm_excel"].fillna(0) > 0) & (diag["cm_lake"].fillna(0) == 0)).sum()):,}')

    # Гипотеза: cm_excel = cm_lake * retl_cnt
    valid = diag[(diag['cm_lake'].fillna(0) > 0) & (diag['cm_excel'].fillna(0) > 0)].copy()
    if len(valid):
        valid['ratio_excel_lake'] = valid['cm_excel'] / valid['cm_lake']
        print(f'\nДоговоры с cm_lake>0 и cm_excel>0: {len(valid):,}')
        print('Распределение отношения cm_excel / cm_lake:')
        print(f'  median: {valid["ratio_excel_lake"].median():.3f}')
        print(f'  mean:   {valid["ratio_excel_lake"].mean():.3f}')
        print(f'  min:    {valid["ratio_excel_lake"].min():.3f}')
        print(f'  max:    {valid["ratio_excel_lake"].max():.3f}')
        print('\nЕсли median ≈ retl_cnt (например 1, 2, 3) — формула в Excel = cm_lake * retl_cnt')
        print('Сравним ratio с retl_cnt_excel для тех же договоров:')
        print(f'  median(retl_cnt_excel): {valid["rc_excel"].median():.0f}')
        print(f'  median(retl_cnt_lake):  {valid["rc_lake"].median():.0f}')
        print(f'  median(ratio):          {valid["ratio_excel_lake"].median():.3f}')

        # Проверка гипотезы cm_excel = cm_lake * retl_cnt_excel
        valid['hypo_cm_lake_x_retl'] = valid['cm_lake'] * valid['rc_excel']
        valid['delta_hypo'] = valid['hypo_cm_lake_x_retl'] - valid['cm_excel']
        print(f'\nГипотеза: cm_excel = cm_lake * retl_cnt_excel')
        print(f'  Сумма cm_excel:                  {valid["cm_excel"].sum():,.0f}')
        print(f'  Сумма (cm_lake * retl_cnt_excel): {valid["hypo_cm_lake_x_retl"].sum():,.0f}')
        print(f'  Дельта:                            {valid["delta_hypo"].sum():,.0f}')
        ratio_hypo = valid['hypo_cm_lake_x_retl'].sum() / valid['cm_excel'].sum() if valid['cm_excel'].sum() else None
        if ratio_hypo:
            print(f'  Ratio (hypo / excel): {ratio_hypo:.3f} (1.0 = идеальное совпадение)')

    print('\nТОП-20 договоров по |delta_cm|:')
    top_cm = diag.reindex(diag['delta_cm'].abs().sort_values(ascending=False).index)
    show_cm = ['agr_id', 'cm_lake', 'cm_excel', 'delta_cm', 'rc_lake', 'rc_excel']
    show_cm = [c for c in show_cm if c in top_cm.columns]
    display(top_cm[show_cm].head(20))

    print('\n' + '=' * 70)
    print('ДИАГНОСТИКА: retl_cnt (фан-аут?)')
    print('=' * 70)

    # Считаем число agr_id на n_cmp_client в lake
    if 'n_cmp_client' in lake_check_df.columns:
        cmp_to_agr = (
            lake_check_df.groupby('n_cmp_client')['agr_id']
            .nunique()
            .reset_index()
            .rename(columns={'agr_id': 'agr_cnt_per_cmp'})
        )

        diag_with_cmp = diag.merge(
            lake_check_df[['agr_id', 'n_cmp_client']].drop_duplicates(),
            on='agr_id', how='left'
        ).merge(cmp_to_agr, on='n_cmp_client', how='left')

        print('\nРаспределение договоров по числу agr_id у одной компании:')
        print(diag_with_cmp['agr_cnt_per_cmp'].value_counts().sort_index().head(15))

        # Для компаний с >1 agr_id - получают ли разные agr_id один и тот же retl_cnt?
        multi_cmp = diag_with_cmp[diag_with_cmp['agr_cnt_per_cmp'] > 1]
        if len(multi_cmp):
            print(f'\nДоговоры в компаниях с >1 agr_id: {len(multi_cmp):,}')
            same_retl = multi_cmp.groupby('n_cmp_client')['rc_lake'].nunique() == 1
            print(f'  Компаний где ВСЕ agr_id имеют ОДИНАКОВЫЙ retl_cnt_lake: {int(same_retl.sum()):,} из {len(same_retl):,}')
            print('  Если эта цифра близка к 100% — это подтверждает фан-аут (все agr_id одной компании получают весь набор ТТ)')

    print('\nТОП-20 договоров по |delta_rc|:')
    display(diag_with_cmp.reindex(diag_with_cmp['delta_rc'].abs().sort_values(ascending=False).index)[
        ['agr_id', 'n_cmp_client', 'agr_cnt_per_cmp', 'rc_lake', 'rc_excel', 'delta_rc']
    ].head(20))
else:
    print('Диагностика пропущена (нет данных в both_df).')
