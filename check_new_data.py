import logging
import pathlib
import sys
import warnings
from datetime import date, datetime, time, timedelta, timezone

from hydra import compose, initialize
from rail_connectors import connect

warnings.filterwarnings("ignore")


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Журнал загрузок и расписание DAG в московском времени.
MSK = timezone(timedelta(hours=3))


def main(
    *,
    config_path: str = "../../conf",
    config_name: str = "config",
    overrides: list[str] = [],
    allow_skip: bool = False,
):
    logger.info("\nTask script path with name: %s", f"{__file__}")
    with initialize(version_base=None, config_path=config_path):
        conf = compose(config_name=config_name, overrides=overrides)
    cfg = conf.env

    schedule_table = cfg.tables.impala.tables.schedule_table
    run_names = [str(name).lower() for name in cfg.acq_sensor_run_names]
    # Данные за exec_dt (вчера) потоки ODS грузят на следующий день.
    load_day = date.fromisoformat(str(cfg.exec_dt)) + timedelta(days=1)
    logger.info("\nschedule_table: %s, load_day: %s", schedule_table, load_day)

    # Временный сбой Impala не должен ронять сенсор: ждём следующей проверки, пока не истёк timeout.
    try:
        dl = connect(**cfg.session_config.impala)
        with dl:
            dl.execute(f"refresh {schedule_table}")
        with dl:
            loaded = dl.fetch(f"""
                SELECT DISTINCT lower(run_name) AS run_name
                FROM {schedule_table}
                WHERE run_status = 'Succeeded'
                    AND CAST(run_start_dt AS TIMESTAMP) >= CAST('{load_day}' AS TIMESTAMP)
                    AND lower(run_name) IN ({", ".join(f"'{name}'" for name in run_names)})
            """)
    except Exception as exc:
        logger.warning("\nНе удалось проверить журнал загрузок, повторим при следующей проверке: %s", exc)
        return False

    done = set() if loaded is None or loaded.empty else set(loaded["run_name"])
    missing = sorted(set(run_names) - done)
    if missing:
        logger.info("\nЕщё не загрузились: %s", missing)
        skip_at = cfg.get("acq_sensor_skip_at")
        if allow_skip and skip_at:
            deadline = datetime.combine(load_day, time.fromisoformat(str(skip_at)), MSK)
            if datetime.now(MSK) >= deadline:
                logger.warning(
                    "\nНе дождались к %s: %s. Считаем на последних загруженных данных этих источников.",
                    deadline.strftime("%d.%m %H:%M"), missing,
                )
                return True
        return False
    logger.info("\nВсе потоки загрузились")
    return True


if __name__ == "__main__":
    logger.addHandler(logging.StreamHandler(sys.stdout))
    overrides = sys.argv[1:]
    overrides.append(f"project_path={pathlib.Path().resolve()}")
    print(main(overrides=overrides))
