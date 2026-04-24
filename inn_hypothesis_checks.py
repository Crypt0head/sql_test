"""
Checks for hypothesis:
  - merchants represent retail points (TSP/retl)
  - INN represents client level

How to run in notebook:
  1) Ensure connection object `imp` is initialized.
  2) %run "E:/DTB(dashbord)/DAGS/inn_hypothesis_checks.py"
  3) Execute: run_all_checks(imp, target_inn="2204035492")
"""


def run_all_checks(imp, target_inn: str = "2204035492"):
    """Run all checks and return results as dict of DataFrames."""
    results = {}

    # 1) INN -> count of retail points and agreements (wide check)
    with imp:
        results["inn_to_retl_and_agr_counts"] = imp.fetch("""
            select
              c.c_inn,
              count(distinct m.c_nmrc) as retl_cnt,
              count(distinct a.c_agr_number) as agr_cnt
            from ods_alpha.scd1_agreements a
            join ods_alpha.scd1_companies c
              on c.n_cmp = a.n_cmp_client
            left join ods_alpha.scd1_merchants m
              on m.n_cmp = a.n_cmp_client
            where a.acq_class = 'SA'
            group by c.c_inn
            having count(distinct m.c_nmrc) > 1
            order by retl_cnt desc
            limit 50
        """)

    # 2) One INN -> list of retail points
    with imp:
        results["retl_list_for_inn"] = imp.fetch(f"""
            select
              c.c_inn,
              m.c_nmrc as retl_id,
              m.c_mrc_name as retl_name,
              m.n_mcc
            from ods_alpha.scd1_agreements a
            join ods_alpha.scd1_companies c
              on c.n_cmp = a.n_cmp_client
            left join ods_alpha.scd1_merchants m
              on m.n_cmp = a.n_cmp_client
            where c.c_inn = '{target_inn}'
              and a.acq_class = 'SA'
            order by m.c_nmrc
        """)

    # 3) One INN -> distinct retail points in transactions
    with imp:
        results["trx_retl_count_for_inn"] = imp.fetch(f"""
            select
              c.c_inn,
              count(distinct t.c_nmrc) as trx_retl_cnt,
              count(distinct t.n_trx) as trx_cnt
            from ods_alpha.scd1_agreements a
            join ods_alpha.scd1_companies c
              on c.n_cmp = a.n_cmp_client
            join ods_alpha.scd1_trx_acq ta
              on ta.n_agr = a.n_agr
            join ods_alpha.scd1_trx t
              on t.n_trx = ta.n_trx
            where c.c_inn = '{target_inn}'
              and a.acq_class = 'SA'
              and t.c_trx_class = 'SA'
              and t.c_trx_type = 'S01'
              and t.c_nter is not null
              and t.cf_trx_stat <> 'R'
            group by c.c_inn
        """)

    # 4) Reverse check: one retail point mapped to multiple INN (anomaly check)
    with imp:
        results["retl_to_many_inn_anomalies"] = imp.fetch("""
            select
              m.c_nmrc as retl_id,
              count(distinct c.c_inn) as inn_cnt
            from ods_alpha.scd1_merchants m
            join ods_alpha.scd1_agreements a
              on a.n_cmp_client = m.n_cmp
            join ods_alpha.scd1_companies c
              on c.n_cmp = a.n_cmp_client
            where a.acq_class = 'SA'
            group by m.c_nmrc
            having count(distinct c.c_inn) > 1
            order by inn_cnt desc
            limit 50
        """)

    return results


def print_summary(results):
    """Small text summary for quick interpretation."""
    print("=== CHECK SUMMARY ===")
    print(f"1) INN->retl sample rows: {len(results['inn_to_retl_and_agr_counts'])}")
    print(f"2) Retail list rows for target INN: {len(results['retl_list_for_inn'])}")
    print(f"3) trx_retl_count rows: {len(results['trx_retl_count_for_inn'])}")
    print(f"4) Retail-to-many-INN anomaly rows: {len(results['retl_to_many_inn_anomalies'])}")

