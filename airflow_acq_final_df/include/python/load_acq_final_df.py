import calendar
import logging
import pathlib
import sys
import warnings
from datetime import datetime

from hydra import compose, initialize
from rail_connectors import connect

warnings.filterwarnings("ignore")


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# (table key in table_conf.yaml / sql file name, MEM_LIMIT, may the table be empty)
PARTS = [
    ("acq_01_sa_perimeter", "8g", False),
    ("acq_02_cdi_map", "8g", False),
    ("acq_03_cft_map", "8g", False),
    ("acq_05_trx_agr", "16g", False),
    ("acq_mcc_by_agr", "4g", False),
    ("acq_04_terminals", "16g", True),
    ("acq_05b_active_retl", "8g", True),
    ("acq_06_cft_attrs", "8g", True),
    ("acq_08b_tariff_fix", "8g", True),
    ("acq_09_tariff", "8g", True),
    ("acq_10o_doc_oper", "8g", True),
    ("acq_10m_mpos", "8g", True),
    ("acq_10b_products", "8g", True),
    ("acq_kedr_chod", "4g", True),
    ("acq_month_slice", "8g", False),
]


def month_params(year, month):
    report_month = f"{year}-{month:02d}"
    return {
        "report_month": report_month,
        "month_start": f"{report_month}-01",
        "month_end": f"{report_month}-{calendar.monthrange(year, month)[1]:02d}",
        "yearmm": f"{year}{month:02d}",
    }


def months_to_load(exec_dt, prev_month_days):
    day = datetime.strptime(exec_dt, "%Y-%m-%d")
    months = []
    if day.day <= prev_month_days:
        prev_year, prev_month = (day.year - 1, 12) if day.month == 1 else (day.year, day.month - 1)
        months.append(month_params(prev_year, prev_month))
    months.append(month_params(day.year, day.month))
    return months


def count_rows(imp, table):
    df = imp.fetch(f"SELECT count(*) AS n FROM {table}")
    return int(df.iloc[0, 0])


def build_month(imp, sql_dir, tbls, params):
    for key, mem_limit, allow_empty in PARTS:
        table = tbls[key]
        logger.info("\nBuild %s for %s", table, params["report_month"])
        with imp:
            imp.execute(f"SET MEM_LIMIT={mem_limit}")
            imp.execute(f"DROP TABLE IF EXISTS {table}")
            imp.execute_from_file(f"{sql_dir}/{key}.sql", **tbls, **params)
            n_rows = count_rows(imp, table)
        logger.info("\n%s rows: %s", table, n_rows)
        if n_rows == 0 and not allow_empty:
            raise RuntimeError(f"Empty table {table} for {params['report_month']}")

    with imp:
        n_slice = count_rows(imp, tbls.acq_month_slice)
        n_perimeter = count_rows(imp, tbls.acq_01_sa_perimeter)
    if n_slice != n_perimeter:
        raise RuntimeError(
            f"Slice {params['report_month']}: {n_slice} rows, perimeter {n_perimeter}. "
            "A join multiplied agreements."
        )


def main(
    *,
    config_path: str = "../../conf",
    config_name: str = "config",
    cfg: list[str] = [],
):
    logger.info('\nTask script path with name: %s', f'{__file__}')
    with initialize(version_base=None, config_path=config_path):
        cfg = compose(config_name=config_name, overrides=cfg)
    project_path = cfg.project_path
    logger.info("\nProject path: %s", f"{project_path}")
    cfg = cfg.env

    imp = connect(**cfg.session_config.impala)
    tbls = cfg.tables.impala.tables
    sql_dir = f"{project_path}/include/sql"
    slice_load = tbls.acq_slice_load

    months = months_to_load(cfg.exec_dt, int(cfg.acq_prev_month_days))
    logger.info("\nMonths to load: %s", [m["report_month"] for m in months])

    with imp:
        imp.execute(f"DROP TABLE IF EXISTS {slice_load}")

    for i, params in enumerate(months):
        build_month(imp, sql_dir, tbls, params)
        with imp:
            if i == 0:
                imp.execute(
                    f"CREATE TABLE {slice_load} STORED AS PARQUET "
                    f"TBLPROPERTIES('transactional'='false') "
                    f"AS SELECT * FROM {tbls.acq_month_slice}"
                )
            else:
                imp.execute(f"INSERT INTO {slice_load} SELECT * FROM {tbls.acq_month_slice}")
            logger.info("\n%s rows after %s: %s", slice_load, params["report_month"], count_rows(imp, slice_load))


if __name__ == "__main__":
    logger.addHandler(logging.StreamHandler(sys.stdout))
    overrides = sys.argv[1:]
    overrides.append(f"project_path={pathlib.Path().resolve()}")
    main(cfg=overrides)
