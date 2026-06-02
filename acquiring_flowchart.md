# Datamart Month Acquiring - блок-схема

```mermaid
flowchart TD
    A[Старт: параметры запуска<br/>report_month, output_csv_path] --> B[Расчет дат периода<br/>month_start, month_end, report_month_label]
    B --> C[Invalidate metadata<br/>по списку таблиц]
    C --> D[SA-периметр<br/>agreements + companies<br/>фильтр SA + валидность договора]

    D --> E[Нормализация ключей<br/>INN, contract_number]
    E --> F[INN -> CDI<br/>OCRM: s_org_ext + сегмент SSP]
    F --> G[CDI -> CFT<br/>ext_id_org (CFT)]

    D --> H[Company metrics<br/>retl_cnt, term_cnt, amortization]

    D --> I[TRX metrics по n_agr<br/>trx_cnt, trx_sum, commission_from_ops,<br/>int_component, active_term_cnt<br/><br/>Условия валидности trx:<br/>- d_trx_orig в пределах месяца<br/>- c_nter is not null<br/>- ods_deleted_flg != 1<br/>- c_trx_class = SA<br/>- c_trx_type = S01<br/>- cf_trx_stat != R<br/>- fiid_grp = RSHB]

    G --> J[R2-атрибуты по cft_id<br/>ogrn, filial_rf, vsp_name, vsp_code,<br/>tariff_name, commission_monthly]

    J --> K[Финальная сборка (витрина)<br/>left merge всех слоев]
    H --> K
    I --> K
    G --> K
    F --> K
    D --> K

    K --> L[Fallback agr_id<br/>если agr_id пуст -> r2_id]
    L --> M[Расчет производных метрик]
    M --> M1[commission_total = commission_from_ops + commission_monthly]
    M1 --> M2[aur = (term_cnt или retl_cnt) * 1926]
    M2 --> M3[chod = commission_total + int_component]
    M3 --> N[Создание финальной витрины]
```

