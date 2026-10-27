CREATE TABLE {acq_04_terminals}
AS
with sa_agr as (
  select distinct
    m.report_month,
    m.month_start,
    m.month_end,
    cast(a.n_agr as string) as n_agr,
    cast(a.n_cmp_client as string) as n_cmp_client
  from ods_alpha.scd1_agreements a
  join ods_alpha.scd1_companies c
    on c.n_cmp = a.n_cmp_client
  join {acq_00_months} m
    on cast(a.d_valid_from as date) <= m.month_end
   and (a.d_valid_to is null or cast(a.d_valid_to as date) >= m.month_start)
  left semi join (
      select distinct
        cast(t.n_agr as string) as n_agr,
        tm.report_month
      from ods_alpha.scd1_agr_terms t
      join {acq_00_months} tm
        on cast(t.d_valid_from as date) <= tm.month_end
       and (t.d_valid_to is null or cast(t.d_valid_to as date) >= tm.month_start)
      where upper(trim(cast(t.cf_ter_type as string))) = 'P'
        and coalesce(t.ods_deleted_flg, '0') <> '1'
    ) pt
    on pt.n_agr = cast(a.n_agr as string)
   and pt.report_month = m.report_month
  where upper(trim(cast(a.acq_class as string))) = 'SA'
    and coalesce(a.ods_deleted_flg, '0') <> '1'
    and coalesce(c.ods_deleted_flg, '0') <> '1'
    and c.c_inn is not null
),

terms_547 as (
  select distinct
    sa.report_month,
    sa.month_start,
    sa.n_agr,
    sa.n_cmp_client,
    cast(t.c_nmrc as string) as c_nmrc
  from sa_agr sa
  join ods_alpha.scd1_agr_terms t
    on cast(t.n_agr as string) = sa.n_agr
  join ods_alpha.scd1_merchants m
    on cast(m.c_nmrc as string) = cast(t.c_nmrc as string)
  where t.c_nmrc is not null
    and upper(coalesce(trim(cast(m.c_mrc_name as string)), '')) not like 'REZERVNYI TERMINAL%'
    and coalesce(upper(trim(cast(m.acq_class as string))), 'NA') <> 'SV'
    and cast(t.d_valid_from as date) <= sa.month_end
    and (t.d_valid_to is null or cast(t.d_valid_to as date) >= sa.month_start)
    and coalesce(t.ods_deleted_flg, '0') <> '1'
    and coalesce(m.ods_deleted_flg, '0') <> '1'
),
pos_period as (
  select distinct
    m.report_month,
    cast(p.c_nmrc as string) as c_nmrc,
    cast(p.c_pos_serial as string) as c_pos_serial,
    cast(p.c_nter as string) as c_nter
  from ods_alpha.scd1_pos_terminals p
  join {acq_00_months} m
    on cast(p.d_ter_install as date) < date_add(m.month_end, 1)
   and (p.d_ter_close is null or cast(p.d_ter_close as date) >= m.month_start)
  where p.c_pos_serial is not null
    and cast(p.d_ter_install as date) is not null
    and coalesce(p.ods_deleted_flg, '0') <> '1'
),
term_active_547 as (
  select
    t.report_month,
    t.month_start,
    t.n_agr,
    t.n_cmp_client,
    t.c_nmrc,
    p.c_pos_serial,
    p.c_nter
  from terms_547 t
  left join pos_period p
    on p.c_nmrc = t.c_nmrc
   and p.report_month = t.report_month
),
retl_547 as (
  select report_month, n_agr, count(distinct c_nmrc) as retl_cnt_547
  from term_active_547
  group by report_month, n_agr
),
retl_with_term_547 as (
  select report_month, n_agr, count(distinct c_nmrc) as retl_with_term_cnt_547
  from term_active_547
  where c_pos_serial is not null or c_nter is not null
  group by report_month, n_agr
),
term_547 as (
  select report_month, n_agr, count(distinct c_pos_serial) as term_cnt_547
  from term_active_547
  group by report_month, n_agr
),
-- One physical device (serial) -> one owner n_agr -> one amort amount.
-- Avoids multi-agr and mid-month c_nter rename double-count.
amort_serial_owner_547 as (
  select report_month, month_start, n_agr, c_nter, c_pos_serial
  from (
    select
      report_month,
      month_start,
      cast(n_agr as string) as n_agr,
      cast(c_nter as string) as c_nter,
      cast(c_pos_serial as string) as c_pos_serial,
      row_number() over (
        partition by
          report_month,
          coalesce(
            nullif(trim(cast(c_pos_serial as string)), ''),
            concat('__NTER__', cast(c_nter as string))
          )
        order by
          cast(n_agr as string),
          cast(c_nter as string)
      ) as rn
    from term_active_547
    where c_nter is not null
  ) z
  where rn = 1
),
amort_547 as (
  select
    o.report_month,
    o.n_agr,
    sum(coalesce(cast(am.amortization_for_report_month as double), 0.0)) as amortization_547
  from amort_serial_owner_547 o
  left join sandbox_ai.shestopalov_terminal_amortization_model_jan_aug am
    on cast(am.c_nter as string) = o.c_nter
   and cast(am.snapshot_month_start as date) = o.month_start
  group by o.report_month, o.n_agr
),

fallback_agrs as (
  select sa.report_month, sa.month_start, sa.month_end, sa.n_agr, sa.n_cmp_client
  from sa_agr sa
  left join retl_547 r547 on r547.n_agr = sa.n_agr and r547.report_month = sa.report_month
  left join term_547 t547 on t547.n_agr = sa.n_agr and t547.report_month = sa.report_month
  where r547.n_agr is null and t547.n_agr is null
),
old_nmrc as (
  select fa.report_month, fa.month_start, fa.month_end, fa.n_agr, fa.n_cmp_client, cast(mm.c_nmrc as string) as c_nmrc
  from fallback_agrs fa
  join ods_alpha.scd1_merchants mm
    on cast(mm.n_cmp as string) = fa.n_cmp_client
  where mm.c_nmrc is not null
    and coalesce(mm.ods_deleted_flg, '0') <> '1'
  group by fa.report_month, fa.month_start, fa.month_end, fa.n_agr, fa.n_cmp_client, cast(mm.c_nmrc as string)
),
old_term_active as (
  select
    o.report_month,
    o.month_start,
    o.n_agr,
    o.n_cmp_client,
    o.c_nmrc,
    cast(t.c_nter as string) as c_nter,
    cast(t.c_pos_serial as string) as c_pos_serial
  from old_nmrc o
  join ods_alpha.scd1_pos_terminals t
    on cast(t.c_nmrc as string) = o.c_nmrc
  where t.c_nter is not null
    and coalesce(t.ods_deleted_flg, '0') <> '1'
    and cast(t.d_ter_install as date) is not null
    and cast(t.d_ter_install as date) <= o.month_end
    and coalesce(cast(t.d_ter_close as date), cast('2999-12-31' as date)) >= o.month_start
  group by o.report_month, o.month_start, o.n_agr, o.n_cmp_client, o.c_nmrc, cast(t.c_nter as string), cast(t.c_pos_serial as string)
),
old_retl as (
  select report_month, n_agr, count(distinct c_nmrc) as retl_cnt_old
  from old_term_active
  group by report_month, n_agr
),
old_term as (
  select report_month, n_agr, count(distinct c_nter) as term_cnt_old
  from old_term_active
  group by report_month, n_agr
),
amort_serial_owner_old as (
  select report_month, month_start, n_agr, c_nter, c_pos_serial
  from (
    select
      report_month,
      month_start,
      cast(n_agr as string) as n_agr,
      cast(c_nter as string) as c_nter,
      cast(c_pos_serial as string) as c_pos_serial,
      row_number() over (
        partition by
          report_month,
          coalesce(
            nullif(trim(cast(c_pos_serial as string)), ''),
            concat('__NTER__', cast(c_nter as string))
          )
        order by
          cast(n_agr as string),
          cast(c_nter as string)
      ) as rn
    from old_term_active
    where c_nter is not null
  ) z
  where rn = 1
),
old_amort as (
  select
    o.report_month,
    o.n_agr,
    sum(coalesce(cast(am.amortization_for_report_month as double), 0.0)) as amortization_old
  from amort_serial_owner_old o
  left join sandbox_ai.shestopalov_terminal_amortization_model_jan_aug am
    on cast(am.c_nter as string) = o.c_nter
   and cast(am.snapshot_month_start as date) = o.month_start
  group by o.report_month, o.n_agr
)

select
  sa.report_month,
  sa.n_agr,
  sa.n_cmp_client,
  coalesce(r547.retl_cnt_547, rold.retl_cnt_old) as retl_cnt,
  coalesce(rwt547.retl_with_term_cnt_547, rold.retl_cnt_old, 0) as retl_with_term_cnt,
  coalesce(t547.term_cnt_547, told.term_cnt_old) as term_cnt,
  coalesce(a547.amortization_547, aold.amortization_old, 0.0) as amortization,
  case
    when r547.n_agr is not null or t547.n_agr is not null then '547_like'
    when rold.n_agr is not null or told.n_agr is not null then 'fallback_old'
    else 'no_data'
  end as term_calc_source
from sa_agr sa
left join retl_547 r547 on r547.n_agr = sa.n_agr and r547.report_month = sa.report_month
left join retl_with_term_547 rwt547 on rwt547.n_agr = sa.n_agr and rwt547.report_month = sa.report_month
left join term_547 t547 on t547.n_agr = sa.n_agr and t547.report_month = sa.report_month
left join amort_547 a547 on a547.n_agr = sa.n_agr and a547.report_month = sa.report_month
left join old_retl rold on rold.n_agr = sa.n_agr and rold.report_month = sa.report_month
left join old_term told on told.n_agr = sa.n_agr and told.report_month = sa.report_month
left join old_amort aold on aold.n_agr = sa.n_agr and aold.report_month = sa.report_month
