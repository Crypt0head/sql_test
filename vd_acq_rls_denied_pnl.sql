-- Virtual dataset: vd_acq_rls_denied_pnl
-- Jinja ON. Всегда 1 строка. Дату обновления меняй в литерале ниже.

SELECT
  CASE
    WHEN EXISTS (
      SELECT 1
      FROM sbx_da.rls_acq_user u
      JOIN sbx_da.rls_acq_role_sheet s
        ON BTRIM(CAST(s.role AS TEXT)) = BTRIM(CAST(u.role AS TEXT))
      WHERE lower(BTRIM(CAST(u.username AS TEXT))) = lower(BTRIM('{{ current_username() }}'))
        AND BTRIM(CAST(s.pnl AS TEXT)) IN ('1', 'true', 'True', 'Y', 'y')
    )
    THEN 'Данные обновлены: 25.09.2026 07:00'
    ELSE 'Тебе сюда нельзя'
  END AS access_message,
  CASE
    WHEN EXISTS (
      SELECT 1
      FROM sbx_da.rls_acq_user u
      JOIN sbx_da.rls_acq_role_sheet s
        ON BTRIM(CAST(s.role AS TEXT)) = BTRIM(CAST(u.role AS TEXT))
      WHERE lower(BTRIM(CAST(u.username AS TEXT))) = lower(BTRIM('{{ current_username() }}'))
        AND BTRIM(CAST(s.pnl AS TEXT)) IN ('1', 'true', 'True', 'Y', 'y')
    )
    THEN 'Срез витрины (дата заглушка)'
    ELSE 'Нет прав на лист «P&L». Обратитесь к владельцу дашборда.'
  END AS access_detail
;
