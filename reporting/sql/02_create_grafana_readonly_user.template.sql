-- Template for the Grafana database user.
-- Run after 01_create_transfer_package_metrics_view.sql, while connected to
-- the schema that contains v_transfer_package_metrics.
-- Replace CHANGE_ME_STRONG_PASSWORD before running.

SET @schema_name := DATABASE();
SET @grafana_user := 'grafana_transfer_ro';
SET @grafana_host := '%';
SET @grafana_password := 'CHANGE_ME_STRONG_PASSWORD';

SET @create_user_sql := CONCAT(
  'CREATE USER IF NOT EXISTS ',
  QUOTE(@grafana_user),
  '@',
  QUOTE(@grafana_host),
  ' IDENTIFIED BY ',
  QUOTE(@grafana_password)
);

PREPARE create_user_stmt FROM @create_user_sql;
EXECUTE create_user_stmt;
DEALLOCATE PREPARE create_user_stmt;

SET @grant_sql := CONCAT(
  'GRANT SELECT ON `',
  REPLACE(@schema_name, '`', '``'),
  '`.`v_transfer_package_metrics` TO ',
  QUOTE(@grafana_user),
  '@',
  QUOTE(@grafana_host)
);

PREPARE grant_stmt FROM @grant_sql;
EXECUTE grant_stmt;
DEALLOCATE PREPARE grant_stmt;

SET @grant_parameter_sql := CONCAT(
  'GRANT SELECT ON `',
  REPLACE(@schema_name, '`', '``'),
  '`.`v_transfer_parameter_metrics` TO ',
  QUOTE(@grafana_user),
  '@',
  QUOTE(@grafana_host)
);

PREPARE grant_parameter_stmt FROM @grant_parameter_sql;
EXECUTE grant_parameter_stmt;
DEALLOCATE PREPARE grant_parameter_stmt;

SET @grant_event_sql := CONCAT(
  'GRANT SELECT ON `',
  REPLACE(@schema_name, '`', '``'),
  '`.`v_transfer_event_metrics` TO ',
  QUOTE(@grafana_user),
  '@',
  QUOTE(@grafana_host)
);

PREPARE grant_event_stmt FROM @grant_event_sql;
EXECUTE grant_event_stmt;
DEALLOCATE PREPARE grant_event_stmt;
