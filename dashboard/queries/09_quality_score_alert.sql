-- =============================================================================
-- QUALITY SCORE: Alert Evaluation
-- =============================================================================
-- Purpose: One row, one number, for the quality_score_drop alert. An alert
--          evaluates a single value, and the KPI query returns four rows in
--          mixed units, so the alert reads the governed daily score instead.
--          The definition lives in 08_metric_views.sql; this only selects it.
-- Widget:  None. Referenced from config/alerts.yaml.
-- =============================================================================

SELECT
  date,
  quality_score,
  total_checks,
  failed
FROM medallion_demo.gold.mv_quality_score_daily
ORDER BY date DESC
LIMIT 1;
