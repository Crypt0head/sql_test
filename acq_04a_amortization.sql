CREATE TABLE {acq_04a_amortization}
AS
-- Амортизация терминала за месяц окна, как в 01_07_build_amortization_drp.ipynb:
-- модель (dwh.bmrt_bm_det_device: сначала id_device, потом code_device) -> цена (справочник из term_model.xlsx) ->
-- price/48 в течение 48 месяцев с месяца первой поставки серийника; исключённые модели = 0.
with terminals as (
  select
    m.month_start,
    m.report_month,
    cast(t.c_nter as string) as c_nter,
    cast(t.c_pos_serial as string) as c_pos_serial,
    row_number() over (
      partition by m.month_start, cast(t.c_nter as string)
      order by
        coalesce(cast(t.d_ter_install as date), cast('1900-01-01' as date)) desc,
        coalesce(cast(t.d_ter_close as date), cast('2999-12-31' as date)) desc,
        cast(t.c_pos_serial as string) desc
    ) as rn
  from ods_alpha.scd1_pos_terminals t
  join {acq_00_months} m
    on coalesce(cast(t.d_ter_install as date), cast('1900-01-01' as date)) <= m.month_end
   and coalesce(cast(t.d_ter_close as date), cast('2999-12-31' as date)) >= m.month_start
  -- Без даты установки терминал не попадает в acq_04_terminals, а иначе считался бы действующим с 1900 года.
  where t.c_nter is not null
    and t.d_ter_install is not null
    and nullif(trim(cast(t.c_pos_serial as string)), '') is not null
    and coalesce(t.ods_deleted_flg, '0') <> '1'
),
first_delivery as (
  select
    cast(c_pos_serial as string) as c_pos_serial,
    min(cast(d_ter_delivery as date)) as first_d_ter_delivery
  from ods_alpha.scd1_pos_terminals
  where c_pos_serial is not null
    and d_ter_delivery is not null
    and coalesce(ods_deleted_flg, '0') <> '1'
  group by cast(c_pos_serial as string)
),
device as (
  select
    case when trim(cast(id_device as string)) rlike '^[0-9]+$'
         then cast(cast(trim(cast(id_device as string)) as bigint) as string)
         else upper(trim(cast(id_device as string))) end as id_norm,
    case when trim(cast(code_device as string)) rlike '^[0-9]+$'
         then cast(cast(trim(cast(code_device as string)) as bigint) as string)
         else upper(trim(cast(code_device as string))) end as code_norm,
    trim(cast(model_device as string)) as model_device
  from dwh.bmrt_bm_det_device
  where nullif(trim(cast(model_device as string)), '') is not null
    and lower(trim(cast(model_device as string))) not in ('none', 'nan', 'null', '<null>', 'nat')
),
by_id as (
  select id_norm, max(model_device) as model_device from device where id_norm is not null group by id_norm
),
by_code as (
  select code_norm, max(model_device) as model_device from device where code_norm is not null group by code_norm
),
term_model as (
  select
    t.month_start,
    t.report_month,
    t.c_nter,
    t.c_pos_serial,
    fd.first_d_ter_delivery,
    coalesce(i.model_device, c.model_device) as model_device,
    case when i.model_device is not null then 'id_device'
         when c.model_device is not null then 'code_device' end as join_key_used
  from terminals t
  left join first_delivery fd
    on fd.c_pos_serial = t.c_pos_serial
  left join by_id i
    on i.id_norm = case when trim(t.c_nter) rlike '^[0-9]+$'
                        then cast(cast(trim(t.c_nter) as bigint) as string)
                        else upper(trim(t.c_nter)) end
  left join by_code c
    on c.code_norm = case when trim(t.c_nter) rlike '^[0-9]+$'
                          then cast(cast(trim(t.c_nter) as bigint) as string)
                          else upper(trim(t.c_nter)) end
  where t.rn = 1
),
priced as (
  select
    tm.*,
    p.price,
    coalesce(p.is_excluded, 0) as is_excluded,
    -- Месяцы от месяца первой поставки до отчётного месяца.
    case when tm.first_d_ter_delivery is not null
         then (year(tm.month_start) - year(tm.first_d_ter_delivery)) * 12
              + (month(tm.month_start) - month(tm.first_d_ter_delivery)) end as months_from_start
  from term_model tm
  -- lower() в Impala не трогает кириллицу, а ключ справочника опущен целиком (Python), поэтому заглавные буквы меняем вручную.
  left join {terminal_model_price} p
    on p.model_key = trim(regexp_replace(
      replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
      replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
      replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
        lower(tm.model_device),
        'А','а'),'Б','б'),'В','в'),'Г','г'),'Д','д'),'Е','е'),'Ж','ж'),'З','з'),'И','и'),'Й','й'),'К','к'),'Л','л'),
        'М','м'),'Н','н'),'О','о'),'П','п'),'Р','р'),'С','с'),'Т','т'),'У','у'),'Ф','ф'),'Х','х'),'Ц','ц'),'Ч','ч'),
        'Ш','ш'),'Щ','щ'),'Ъ','ъ'),'Ы','ы'),'Ь','ь'),'Э','э'),'Ю','ю'),'Я','я'),'Ё','е'),'ё','е'),
      '[[:space:]]+', ' '))
)
select
  report_month,
  month_start as snapshot_month_start,
  c_nter,
  c_pos_serial,
  first_d_ter_delivery,
  model_device,
  join_key_used,
  price,
  is_excluded,
  months_from_start,
  case
    when first_d_ter_delivery is null then 'missing_deliver'
    when model_device is null then 'missing_model'
    when price is null then 'missing_price'
    else 'complete'
  end as qc_status,
  case
    when is_excluded = 1 or price is null then 0.0
    when months_from_start >= 0 and months_from_start < 48 then cast(price as double) / 48.0
    else 0.0
  end as amortization_for_report_month
from priced
