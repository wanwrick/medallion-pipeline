-- =============================================================================
-- METRIC VIEWS — Governed Business Metric Definitions
-- =============================================================================
-- Purpose: Create governed, reusable metric definitions in Unity Catalog
-- These ensure consistent metrics across all dashboards and reports
-- =============================================================================

-- Metric View: Overall Data Quality Score
-- Definition: Percentage of quality checks passing, measured daily
CREATE OR REPLACE VIEW medallion_demo.gold.mv_quality_score_daily AS
SELECT
  DATE(checked_at) AS date,
  ROUND(
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
    2
  ) AS quality_score,
  COUNT(*) AS total_checks,
  SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed,
  SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed
FROM medallion_demo.gold.data_quality_metrics
GROUP BY DATE(checked_at);

-- Metric View: Table Health Scorecard
-- Definition: Per-table quality composite score
CREATE OR REPLACE VIEW medallion_demo.gold.mv_table_health AS
SELECT
  table_name,
  ROUND(AVG(CASE WHEN check_type = 'completeness' THEN metric_value END), 2)
    AS completeness_score,
  ROUND(AVG(CASE WHEN check_type = 'uniqueness' THEN metric_value END), 2)
    AS uniqueness_score,
  MAX(CASE WHEN check_type = 'freshness' THEN metric_value END)
    AS freshness_minutes,
  MAX(CASE WHEN check_type = 'row_count' THEN metric_value END)
    AS row_count,
  ROUND(
    (
      COALESCE(AVG(CASE WHEN check_type = 'completeness' THEN metric_value END), 100) * 0.4 +
      COALESCE(AVG(CASE WHEN check_type = 'uniqueness' THEN metric_value END), 100) * 0.3 +
      CASE
        WHEN MAX(CASE WHEN check_type = 'freshness' THEN metric_value END) <= 15 THEN 100
        WHEN MAX(CASE WHEN check_type = 'freshness' THEN metric_value END) <= 60 THEN 80
        ELSE 50
      END * 0.3
    ),
    1
  ) AS composite_health_score,
  MAX(checked_at) AS last_checked
FROM medallion_demo.gold.data_quality_metrics
WHERE checked_at >= CURRENT_DATE() - INTERVAL 1 DAY
GROUP BY table_name;

-- Metric View: SLA Compliance Weekly
-- Definition: Percentage of pipeline runs completing within SLA
CREATE OR REPLACE VIEW medallion_demo.gold.mv_sla_compliance AS
SELECT
  DATE_TRUNC('WEEK', DATE(checked_at)) AS week_start,
  COUNT(*) AS total_checks,
  SUM(CASE WHEN metric_value <= threshold THEN 1 ELSE 0 END) AS within_sla,
  ROUND(
    SUM(CASE WHEN metric_value <= threshold THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
    1
  ) AS sla_compliance_pct
FROM medallion_demo.gold.data_quality_metrics
WHERE checked_at >= CURRENT_DATE() - INTERVAL 90 DAY
GROUP BY DATE_TRUNC('WEEK', DATE(checked_at))
ORDER BY week_start DESC;
