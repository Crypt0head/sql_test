-- Smoke ролевой модели. SQL Lab, БЕЗ Jinja.
-- Подставь логин в who. Ожидание: 0 = лист закрыт; >0 = открыт.
--
-- ГО ДТПП (Shestopalov-VYur): все cnt > 0
-- ГО Бизнес: overview/pnl/terminals/mcc = 0; tsp_eff/clients > 0
-- РФ РОЭ: overview/pnl/mcc = 0; tsp_eff/clients/terminals > 0
-- РФ КМ (elina-ad): overview/pnl/terminals/mcc = 0; tsp_eff/clients > 0; РФ = Санкт-Петербургский РФ
--
-- Срез РФ: для КМ distinct filial_filter должен быть только его список
-- (после заливки user_filial).

-- SELECT '{{ current_username() }}' AS who;  -- Jinja ON, сверить с ACL

WITH who AS (SELECT 'Shestopalov-VYur' AS username)
SELECT
  w.username,
  u.role,
  u.is_all_filials,
  (SELECT COUNT(*)
   FROM sbx_da.tmp_shestopalov_acq_datamart_final_script_2 d
   WHERE EXISTS (
     SELECT 1 FROM sbx_da.rls_acq_user uu
     JOIN sbx_da.rls_acq_role_sheet s ON BTRIM(CAST(s.role AS TEXT)) = BTRIM(CAST(uu.role AS TEXT))
     WHERE lower(BTRIM(CAST(uu.username AS TEXT))) = lower(BTRIM(w.username))
       AND BTRIM(CAST(s.overview AS TEXT)) IN ('1', 'true', 'True', 'Y', 'y')
   )) AS overview_cnt,
  (SELECT COUNT(*)
   FROM sbx_da.tmp_shestopalov_acq_datamart_final_script_2 d
   WHERE EXISTS (
     SELECT 1 FROM sbx_da.rls_acq_user uu
     JOIN sbx_da.rls_acq_role_sheet s ON BTRIM(CAST(s.role AS TEXT)) = BTRIM(CAST(uu.role AS TEXT))
     WHERE lower(BTRIM(CAST(uu.username AS TEXT))) = lower(BTRIM(w.username))
       AND BTRIM(CAST(s.tsp_eff AS TEXT)) IN ('1', 'true', 'True', 'Y', 'y')
   )) AS tsp_eff_cnt,
  (SELECT COUNT(*)
   FROM sbx_da.tmp_shestopalov_acq_datamart_final_script_2 d
   WHERE EXISTS (
     SELECT 1 FROM sbx_da.rls_acq_user uu
     JOIN sbx_da.rls_acq_role_sheet s ON BTRIM(CAST(s.role AS TEXT)) = BTRIM(CAST(uu.role AS TEXT))
     WHERE lower(BTRIM(CAST(uu.username AS TEXT))) = lower(BTRIM(w.username))
       AND BTRIM(CAST(s.pnl AS TEXT)) IN ('1', 'true', 'True', 'Y', 'y')
   )) AS pnl_cnt,
  (SELECT COUNT(*)
   FROM sbx_da.tmp_shestopalov_acq_datamart_final_script_2 d
   WHERE EXISTS (
     SELECT 1 FROM sbx_da.rls_acq_user uu
     JOIN sbx_da.rls_acq_role_sheet s ON BTRIM(CAST(s.role AS TEXT)) = BTRIM(CAST(uu.role AS TEXT))
     WHERE lower(BTRIM(CAST(uu.username AS TEXT))) = lower(BTRIM(w.username))
       AND BTRIM(CAST(s.clients AS TEXT)) IN ('1', 'true', 'True', 'Y', 'y')
   )) AS clients_cnt,
  (SELECT COUNT(*)
   FROM sbx_da.tmp_shestopalov_acq_datamart_final_script_2 d
   WHERE EXISTS (
     SELECT 1 FROM sbx_da.rls_acq_user uu
     JOIN sbx_da.rls_acq_role_sheet s ON BTRIM(CAST(s.role AS TEXT)) = BTRIM(CAST(uu.role AS TEXT))
     WHERE lower(BTRIM(CAST(uu.username AS TEXT))) = lower(BTRIM(w.username))
       AND BTRIM(CAST(s.terminals AS TEXT)) IN ('1', 'true', 'True', 'Y', 'y')
   )) AS terminals_cnt,
  (SELECT COUNT(*)
   FROM sbx_da.tmp_shestopalov_acq_mcc_month m
   WHERE EXISTS (
     SELECT 1 FROM sbx_da.rls_acq_user uu
     JOIN sbx_da.rls_acq_role_sheet s ON BTRIM(CAST(s.role AS TEXT)) = BTRIM(CAST(uu.role AS TEXT))
     WHERE lower(BTRIM(CAST(uu.username AS TEXT))) = lower(BTRIM(w.username))
       AND BTRIM(CAST(s.mcc AS TEXT)) IN ('1', 'true', 'True', 'Y', 'y')
   )) AS mcc_cnt
FROM who w
LEFT JOIN sbx_da.rls_acq_user u
  ON lower(BTRIM(CAST(u.username AS TEXT))) = lower(BTRIM(w.username));

-- Срез филиалов для РФ (подставь логин КМ / РОЭ):
/*
SELECT DISTINCT
  CASE
    WHEN COALESCE(
           NULLIF(NULLIF(BTRIM(CAST(d.filial_rf AS TEXT)), ''), '<NULL>'),
           'Нет информации'
         ) IN ('РФ', 'Нет информации')
    THEN 'ЦРМБ'
    ELSE COALESCE(
           NULLIF(NULLIF(BTRIM(CAST(d.filial_rf AS TEXT)), ''), '<NULL>'),
           'Нет информации'
         )
  END AS filial_filter
FROM sbx_da.tmp_shestopalov_acq_datamart_final_script_2 d
WHERE EXISTS (
  SELECT 1 FROM sbx_da.rls_acq_user_filial f
  WHERE lower(BTRIM(f.username)) = lower(BTRIM('elina-ad'))
    AND BTRIM(f.filial_filter) = CASE
      WHEN COALESCE(
             NULLIF(NULLIF(BTRIM(CAST(d.filial_rf AS TEXT)), ''), '<NULL>'),
             'Нет информации'
           ) IN ('РФ', 'Нет информации')
      THEN 'ЦРМБ'
      ELSE COALESCE(
             NULLIF(NULLIF(BTRIM(CAST(d.filial_rf AS TEXT)), ''), '<NULL>'),
             'Нет информации'
           )
    END
)
ORDER BY 1;
-- ожидание для elina-ad: только Санкт-Петербургский РФ
*/
