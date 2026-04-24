# INN manual checks with imp.fetch

Замените `target_inn` и `month_start` в первой ячейке, затем запускайте блоки по очереди.

## 0) Параметры

```python
target_inn = "2204035492"
month_start = "2026-02-01"  # YYYY-MM-01
```

## 1) Client in companies

```python
with imp:
    q1 = imp.fetch(f"""
        select
          c.n_cmp,
          c.c_inn,
          c.c_cmp_name
        from ods_alpha.scd1_companies c
        where c.c_inn = '{target_inn}'
    """)
q1
```

## 2) Client agreements

```python
with imp:
    q2 = imp.fetch(f"""
        select
          a.n_cmp_client,
          a.abs_agr_id as agr_id,
          a.n_agr,
          a.c_agr_number,
          a.acq_class,
          a.d_valid_from,
          a.d_valid_to
        from ods_alpha.scd1_agreements a
        join ods_alpha.scd1_companies c
          on c.n_cmp = a.n_cmp_client
        where c.c_inn = '{target_inn}'
        order by a.c_agr_number
    """)
q2
```

## 3) OGRN + branch (RF) by agreement

```python
with imp:
    q3 = imp.fetch(f"""
        with agr as (
          select a.abs_agr_id as agr_id
          from ods_alpha.scd1_agreements a
          join ods_alpha.scd1_companies c
            on c.n_cmp = a.n_cmp_client
          where c.c_inn = '{target_inn}'
            and a.acq_class = 'SA'
            and a.abs_agr_id is not null
        )
        select
          m.id as agr_id,
          corp.c_register_gos_reg_num_rec as ogrn,
          br.c_code as branch_cd,
          br.c_shortlabel as branch_nm
        from agr
        join ods.scd1_z_r2_ip_merchants m
          on m.id = agr.agr_id
        left join ods.scd1_z_client cl
          on cl.id = m.c_cl_org
        left join ods.scd1_z_cl_corp corp
          on corp.id = m.c_cl_org
        left join ods.scd1_z_branch br
          on br.id = cl.c_filial
    """)
q3
```

## 4) VSP list by branch (z_depart)

```python
with imp:
    q4 = imp.fetch(f"""
        with branch_ids as (
          select distinct cl.c_filial as branch_id
          from ods_alpha.scd1_agreements a
          join ods_alpha.scd1_companies c
            on c.n_cmp = a.n_cmp_client
          join ods.scd1_z_r2_ip_merchants m
            on m.id = a.abs_agr_id
          left join ods.scd1_z_client cl
            on cl.id = m.c_cl_org
          where c.c_inn = '{target_inn}'
            and a.acq_class = 'SA'
            and a.abs_agr_id is not null
        )
        select
          d.c_filial as branch_id,
          d.c_code as vsp_code,
          d.c_name as vsp_name
        from ods.scd1_z_depart d
        join branch_ids b
          on b.branch_id = d.c_filial
        order by d.c_code
        limit 200
    """)
q4
```

## 5) Client name from trx and MCC (month)

```python
with imp:
    q5 = imp.fetch(f"""
        with agr as (
          select distinct a.n_agr
          from ods_alpha.scd1_agreements a
          join ods_alpha.scd1_companies c
            on c.n_cmp = a.n_cmp_client
          where c.c_inn = '{target_inn}'
            and a.acq_class = 'SA'
        )
        select
          t.c_mrc_name,
          t.n_mcc,
          count(*) as cnt
        from ods_alpha.scd1_trx_acq ta
        join agr on agr.n_agr = ta.n_agr
        join ods_alpha.scd1_trx t
          on t.n_trx = ta.n_trx
        where t.c_trx_class = 'SA'
          and t.c_trx_type = 'S01'
          and t.c_nter is not null
          and t.ods_deleted_flg <> '1'
          and t.cf_trx_stat <> 'R'
          and trunc(to_date(cast(t.d_trx_orig as timestamp)), 'MM') = cast('{month_start}' as date)
        group by t.c_mrc_name, t.n_mcc
        order by cnt desc
        limit 50
    """)
q5
```

## 6) Tariff by agreement (acq_tar_id)

```python
with imp:
    q6 = imp.fetch(f"""
        with agr as (
          select distinct a.n_agr, a.c_agr_number
          from ods_alpha.scd1_agreements a
          join ods_alpha.scd1_companies c
            on c.n_cmp = a.n_cmp_client
          where c.c_inn = '{target_inn}'
            and a.acq_class = 'SA'
        )
        select
          agr.c_agr_number,
          ta.acq_tar_id,
          count(*) as cnt
        from ods_alpha.scd1_trx_acq ta
        join agr on agr.n_agr = ta.n_agr
        group by agr.c_agr_number, ta.acq_tar_id
        order by agr.c_agr_number, cnt desc
    """)
q6
```
