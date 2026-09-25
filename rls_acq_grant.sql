-- Выполнить в Jupyter / DRP под владельцем таблиц (кто делал rls_acq_roles_build).
-- Из SQL Lab под raisa_superset GRANT не пройдёт.
--
-- Кто ты в SQL Lab:
--   SELECT current_user, session_user;

GRANT USAGE ON SCHEMA sbx_da TO raisa_superset;

GRANT SELECT ON TABLE sbx_da.rls_acq_user TO raisa_superset;
GRANT SELECT ON TABLE sbx_da.rls_acq_user_filial TO raisa_superset;
GRANT SELECT ON TABLE sbx_da.rls_acq_role_sheet TO raisa_superset;

-- Личный логин SQL Lab, если current_user не raisa_superset (кавычки обязательны):
GRANT SELECT ON TABLE sbx_da.rls_acq_user TO "Shestopalov-VYur";
GRANT SELECT ON TABLE sbx_da.rls_acq_user_filial TO "Shestopalov-VYur";
GRANT SELECT ON TABLE sbx_da.rls_acq_role_sheet TO "Shestopalov-VYur";

-- GRANT SELECT ON TABLE sbx_da.rls_acq_user TO "<current_user из SQL Lab>";
-- GRANT SELECT ON TABLE sbx_da.rls_acq_user_filial TO "<current_user из SQL Lab>";
-- GRANT SELECT ON TABLE sbx_da.rls_acq_role_sheet TO "<current_user из SQL Lab>";
