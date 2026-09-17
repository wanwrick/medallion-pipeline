-- =============================================================================
-- VOLUME ANOMALY DETECTION — Z-Score Based Row Count Monitoring
-- =============================================================================
-- Purpose: Detect tables with unusual row count changes (>3 std deviations)
-- Widget: Scatter plot or alert table
-- =============================================================================

WITH daily_counts AS (
  SELECT
    table_name,
    DATE(checked_at) AS check_date,
    MAX(metric_value) AS row_count
  FROM medallion_demo.gold.data_quality_metrics
  WHERE check_type = 'row_count'
    AND checked_at >= CURRENT_DATE() - INTERVAL 30 DAY
  GROUP BY table_name, DATE(checked_at)
),

stats AS (
  SELECT
    table_name,
    AVG(row_count) AS avg_count,
    STDDEV(row_count) AS stddev_count,
    COUNT(*) AS days_observed
  FROM daily_counts
  GROUP BY table_name
  HAVING COUNT(*) >= 7  -- Need at least 7 days of history
)

SELECT
  dc.table_name,
  dc.check_date,
  ROUND(dc.row_count, 0) AS row_count,
  ROUND(s.avg_count, 0) AS avg_row_count,
  ROUND(
    CASE
      WHEN s.stddev_count > 0 THEN (dc.row_count - s.avg_count) / s.stddev_count
      ELSE 0
    END,
    2
  ) AS z_score,
  CASE
    WHEN ABS((dc.row_count - s.avg_count) / NULLIF(s.stddev_count, 0)) > 3 THEN '🔴 Anomaly'
    WHEN ABS((dc.row_count - s.avg_count) / NULLIF(s.stddev_count, 0)) > 2 THEN '🟡 Unusual'
    ELSE '🟢 Normal'
  END AS status,
  ROUND((dc.row_count - s.avg_count) / NULLIF(s.avg_count, 0) * 100, 1) AS pct_change
FROM daily_counts dc
JOIN stats s ON dc.table_name = s.table_name
WHERE dc.check_date = CURRENT_DATE() - INTERVAL 1 DAY
ORDER BY ABS((dc.row_count - s.avg_count) / NULLIF(s.stddev_count, 0)) DESC;
