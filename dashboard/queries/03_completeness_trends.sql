-- =============================================================================
-- COMPLETENESS TRENDS — 30-Day Rolling Quality Scores
-- =============================================================================
-- Purpose: Line chart showing quality score trends over time
-- Widget: Line chart with date x-axis, score y-axis, colored by check_type
-- =============================================================================

SELECT
  DATE(checked_at) AS check_date,
  check_type,
  ROUND(AVG(metric_value), 2) AS avg_score,
  COUNT(*) AS num_checks,
  SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed_checks,
  MIN(metric_value) AS min_score,
  MAX(metric_value) AS max_score
FROM medallion_demo.gold.data_quality_metrics
WHERE checked_at >= CURRENT_DATE() - INTERVAL 30 DAY
GROUP BY DATE(checked_at), check_type
ORDER BY check_date DESC, check_type;
