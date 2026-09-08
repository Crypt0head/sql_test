# HOW TO: переключатель периода на **копии** дашборда (без поломки основного)

Один SQL-файл со всем кодом: [`sources/sql/acq_dashboard_period_all.sql`](sources/sql/acq_dashboard_period_all.sql)

**Принцип:** основной дашборд и его чарты **не трогаем**. Все изменения — на копии дашборда + новые virtual datasets + новые чарты с суффиксом `_period`.

---

## Что получится в итоге

| Объект | Основной (prod) | Копия (тест) |
|--------|-----------------|--------------|
| Dashboard | без изменений | `Эквайринг v2 — период (TEST)` |
| Чарты | `v2_filial_*`, `v2_mcc_month` | `v2_period_filial_*`, `v2_period_mcc_month` |
| Datasets | Dataset A / B (physical) | + 3 новых virtual (блоки 1–3) |
| Фильтры | только месяц | месяц + режим периода |

Virtual datasets **не ломают** старые чарты — они только добавляются в Superset.

---

## Часть 0. Подготовка (5 мин)

### 0.1. Скопировать на банковский ПК

Достаточно **двух файлов**:

```
sources/sql/acq_dashboard_period_all.sql
HOW_TO_period_mode_on_dashboard_copy.md   ← этот файл
```

Опционально для сверки метрик: `superset_metrics_dashboard_tables.txt`

### 0.2. Проверить Jinja (блок 0 в SQL-файле)

SQL Lab → database DRP → выполнить:

```sql
SELECT {{ 1 + 1 }} AS jinja_ok;
```

| Результат | Действие |
|-----------|----------|
| `jinja_ok = 2` | продолжаем |
| ошибка синтаксиса | **стоп** — нужен `ENABLE_TEMPLATE_PROCESSING` у админа |

### 0.3. Smoke доступа к данным (блок 8a)

```sql
SELECT COUNT(*) FROM sbx_da.tmp_shestopalov_acq_datamart_final_script_2;
SELECT COUNT(*) FROM sbx_da.tmp_shestopalov_acq_mcc_month;
```

---

## Часть 1. Создать 3 virtual dataset (10–15 мин)

**Важно:** в SQL Lab копируйте **только один блок** из `acq_dashboard_period_all.sql` за раз.

### Dataset 1 — справочник режимов

1. SQL Lab → **блок 1** → Run.
2. **Save → Save dataset** → имя: `vd_period_mode_options`
3. Data → Datasets → `vd_period_mode_options` → **Sync columns**

Колонки: `period_mode`, `period_mode_label`

### Dataset 2 — основная витрина с периодом

1. SQL Lab → **блок 2** → Run (без фильтров на дашборде вернёт последний месяц).
2. Save dataset → `vd_acq_dashboard_period`
3. Sync columns — должны быть `filial_filter`, `fin_result`, `commission_*`, `period_from`, `period_to`

**Calculated column `filial_filter` не создавать** — он уже в SQL.

### Dataset 3 — MCC с периодом

1. SQL Lab → **блок 3** → Save dataset → `vd_mcc_dashboard_period`
2. Sync columns

---

## Часть 2. Скопировать дашборд (2 мин)

1. Откройте **основной** дашборд «Эквайринг…».
2. Меню **⋮** (или Dashboard properties) → **Save as** / **Duplicate**.
3. Имя копии, например: **`Эквайринг v2 — период (TEST)`**
4. Убедитесь, что URL копии **другой** — основной дашборд закройте, больше не редактируйте.

> Если в вашем Superset нет Duplicate — создайте пустой dashboard и перетащите чарты через **Copy chart** (см. часть 3).

---

## Часть 3. Скопировать чарты на копии (15–20 мин)

Для **каждого** из 4 чартов на копии дашборда:

| Исходный чарт | Действие | Новое имя |
|---------------|----------|-----------|
| `v2_filial_tsp_efficiency` | Save as / Duplicate | `v2_period_filial_tsp_efficiency` |
| `v2_filial_pnl_ranking` | Save as / Duplicate | `v2_period_filial_pnl_ranking` |
| `v2_filial_terminals` | Save as / Duplicate | `v2_period_filial_terminals` |
| `v2_mcc_month` | Save as / Duplicate | `v2_period_mcc_month` |

### Как дублировать чарт в Superset

1. Charts → найти чарт → открыть.
2. **Save as** (не Save!) → новое имя с `_period`.
3. На **копии дашборда**: удалить старые чарты, добавить новые `_period`.

**Оригинальные чарты `v2_filial_*` не менять и не удалять.**

---

## Часть 4. Переключить dataset на копиях чартов (10 мин)

Для каждого **нового** чарта `_period`:

### Чарты 1, 1b, 2 (эффективность, P&L, терминалы)

1. Edit chart → **Data** → Dataset → `vd_acq_dashboard_period`
2. **Filters** (на уровне чарта):
   - **Удалить** фильтр по `report_month` / `snapshot_month_start`, если был
   - Диапазон месяцев теперь в SQL dataset
3. **Dimensions** — без изменений: `filial_filter`
4. **Metrics** — без изменений (те же Custom SQL, блоки 4–6 в SQL-файле)
5. Update chart

### Чарт 3 (MCC)

1. Dataset → `vd_mcc_dashboard_period`
2. Удалить chart filter по `report_month`
3. Dimension: `mcc` (или `mcc_label`)
4. **Обязательно** проверить метрики (блок 7):
   - **НЕ** `AVG(acq_pct)` / `AVG(share_trx_sum_pct)` — только взвешенные SUM
   - иначе YTD/Quarter покажут неверные доли

---

## Часть 5. Фильтры на копии дашборда (10 мин)

Edit dashboard (только **TEST** копию) → **Filters**

### Фильтр 1 — Месяц (якорь)

| Поле | Значение |
|------|----------|
| Name | `report_month` |
| Type | Select / Time grain (single value) |
| Dataset | `vd_acq_dashboard_period` |
| Column | `report_month` |
| Default | последний месяц, напр. `2026-08` |
| Required | да |
| Scope | только 4 чарта `_period` на этой копии |

Если на копии остался **старый** filter по месяцу от prod — **удалите** его или сузьте scope, чтобы не конфликтовал.

### Фильтр 2 — Режим периода

| Поле | Значение |
|------|----------|
| Name | `period_mode` |
| Type | Select filter (single value) |
| Dataset | `vd_period_mode_options` |
| Column | `period_mode` |
| Default | `month` |
| Scope | те же 4 чарта `_period` |

**Критично:** имя колонки фильтра = **`period_mode`** (Jinja читает именно так).

Если Select не привязывается к dataset — **Manual values**:

| Value | Label |
|-------|-------|
| `month` | Месяц |
| `ytd` | Нарастающий с начала года |
| `quarter` | Квартал |

Filter key / column name = `period_mode`.

### Filter scope

Убедитесь, что **новые фильтры не применяются** к чартам основного дашборда (если чарты общие).  
Безопасный путь: чарты `_period` существуют **только** на TEST-копии.

---

## Часть 6. Проверка (smoke, 10 мин)

### 6.1. На TEST-дашборде

| Шаг | Действие | Ожидание |
|-----|----------|----------|
| 1 | `period_mode = month`, `report_month = 2026-08` | цифры ≈ как на **основном** дашборде за август |
| 2 | переключить на `ytd` | ДОХОД/P&L **больше**, чем за один месяц |
| 3 | переключить на `quarter` (август) | между month и ytd (июль–август) |
| 4 | MCC: сумма долей | ≈ **100%** во всех режимах |
| 5 | Explore любого `_period` чарта | колонки `period_from`, `period_to` заполнены |

### 6.2. Основной дашборд не тронут

Откройте **prod** дашборд → те же фильтры и цифры, что были до работ.

### 6.3. Сверка P&L (август, режим month)

Красноярск (пример): ДОХОД + РАСХОДЫ ≈ ЧОД; ЧОД ≈ SUM(fin_result).

---

## Часть 7. Публикация на prod (когда TEST OK)

Только после успешного smoke на копии:

1. Повторить части 3–5 **на prod** *или* переименовать TEST-копию в prod и архивировать старый.
2. Рекомендуемый безопасный вариант:
   - оставить старые чарты на prod как fallback (скрытая вкладка «Legacy»)
   - заменить чарты на вкладках v2 на `_period` версии
3. Сообщить пользователям про новый фильтр «Режим периода».

---

## Что **НЕ** делать (чтобы не поломать основной)

| Нельзя | Почему |
|--------|--------|
| Менять SQL у Dataset A/B (physical) | сломает все старые чарты |
| Edit + Save оригинальных `v2_filial_*` | перезапишет prod |
| Удалять virtual datasets prod-графиков | может сломать другие дашборды |
| Менять фильтры на **основном** дашборде во время теста | пользователи увидят полусломанное |
| Запускать весь `acq_dashboard_period_all.sql` одним Run | syntax error (несколько SELECT) |

---

## Rollback

| Проблема | Откат |
|----------|-------|
| TEST-копия не работает | удалить TEST dashboard; prod не тронут |
| `_period` чарты ошибаются | вернуть dataset на Dataset A/B |
| Jinja не работает | оставить prod; period toggle отложить |
| Неверные MCC доли | заменить метрики на блок 7 (SUM, не AVG) |

---

## Логика периода (справка)

Якорь = `report_month` (например `2026-08`):

| period_mode | Диапазон |
|-------------|----------|
| `month` | `2026-08` |
| `ytd` | `2026-01` … `2026-08` |
| `quarter` | `2026-07` … `2026-08` (Q3) |

---

## Интерпретация YTD/Quarter

| Метрика | Поведение |
|---------|-----------|
| Комиссии, оборот, P&L | SUM за диапазон |
| COUNT DISTINCT agr_id | договор учтён, если был **хотя бы в одном** месяце диапазона |
| retl_cnt, term_cnt | SUM по agr×month — **может завышать** на YTD (известное ограничение v1) |

---

## Чеклист

- [ ] Jinja OK (блок 0)
- [ ] 3 virtual dataset созданы
- [ ] TEST dashboard = Duplicate основного
- [ ] 4 чарта `_period` = Save as оригиналов
- [ ] Dataset → `vd_acq_*` / `vd_mcc_*`
- [ ] Chart filters по месяцу **удалены**
- [ ] Dashboard filters: `report_month` + `period_mode`, scope только `_period`
- [ ] Smoke: month ≈ prod; ytd > month; MCC shares ≈ 100%
- [ ] **Основной дашборд проверен — без изменений**
- [ ] (опционально) публикация на prod по части 7
