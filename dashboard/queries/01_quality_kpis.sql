-- =============================================================================
-- DATA QUALITY KPIs — Top-Level Dashboard Counters
-- =============================================================================
-- Purpose: Provides the 4 main KPI widgets for the dashboard header
-- Refresh: Every 5 minutes via dashboard auto-refresh
-- =============================================================================

-- KPI 1: Data Freshness (minutes since last Gold layer update)
SELECT
  'Data Freshness' AS metric_name,
  ROUND(
    TIMESTAMPDIFF(MINUTE, MAX(checked_at), CURRENT_TIMESTAMP()),
    0
  ) AS metric_value,
  'minutes' AS unit,
  CASE
    WHEN TIMESTAMPDIFF(MINUTE, MAX(checked_at), CURRENT_TIMESTAMP()) <= 15 THEN 'good'
    WHEN TIMESTAMPDIFF(MINUTE, MAX(checked_at), CURRENT_TIMESTAMP()) <= 60 THEN 'warning'
    ELSE 'critical'
  END AS status
FROM medallion_demo.gold.data_quality_metrics
WHERE check_type = 'freshness'

UNION ALL

-- KPI 2: Completeness Score (% non-null across critical columns)
SELECT
  'Completeness' AS metric_name,
  ROUND(AVG(metric_value), 1) AS metric_value,
  '%' AS unit,
  CASE
    WHEN AVG(metric_value) >= 99.5 THEN 'good'
    WHEN AVG(metric_value) >= 98.0 THEN 'warning'
    ELSE 'critical'
  END AS status
FROM medallion_demo.gold.data_quality_metrics
WHERE check_type = 'completeness'
  AND checked_at >= CURRENT_DATE() - INTERVAL 1 DAY

UNION ALL

-- KPI 3: Accuracy Score (% of all checks passing)
SELECT
  'Accuracy' AS metric_name,
  ROUND(
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
    1
  ) AS metric_value,
  '%' AS unit,
  CASE
    WHEN SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) >= 99.0 THEN 'good'
    WHEN SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) >= 95.0 THEN 'warning'
    ELSE 'critical'
  END AS status
FROM medallion_demo.gold.data_quality_metrics
WHERE checked_at >= CURRENT_DATE() - INTERVAL 1 DAY

UNION ALL

-- KPI 4: Pipeline SLA (% of checks within threshold)
SELECT
  'Pipeline SLA' AS metric_name,
  ROUND(
    SUM(CASE WHEN metric_value <= threshold THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0),
    1
  ) AS metric_value,
  '%' AS unit,
  CASE
    WHEN SUM(CASE WHEN metric_value <= threshold THEN 1 ELSE 0 END) * 100.0 / COUNT(*) >= 98.0 THEN 'good'
    WHEN SUM(CASE WHEN metric_value <= threshold THEN 1 ELSE 0 END) * 100.0 / COUNT(*) >= 95.0 THEN 'warning'
    ELSE 'critical'
  END AS status
FROM medallion_demo.gold.data_quality_metrics
WHERE checked_at >= CURRENT_DATE() - INTERVAL 7 DAY;
