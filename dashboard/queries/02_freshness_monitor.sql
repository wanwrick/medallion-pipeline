-- =============================================================================
-- FRESHNESS MONITOR — Table-Level Staleness Tracking
-- =============================================================================
-- Purpose: Shows how fresh each table is relative to its SLA
-- Widget: Table with conditional formatting (green/yellow/red)
-- =============================================================================

SELECT
  table_name,
  MAX(checked_at) AS last_updated,
  ROUND(
    TIMESTAMPDIFF(MINUTE, MAX(checked_at), CURRENT_TIMESTAMP()),
    0
  ) AS minutes_stale,
  MAX(threshold) AS sla_minutes,
  CASE
    WHEN TIMESTAMPDIFF(MINUTE, MAX(checked_at), CURRENT_TIMESTAMP()) <= MAX(threshold) * 0.5
      THEN '🟢 Fresh'
    WHEN TIMESTAMPDIFF(MINUTE, MAX(checked_at), CURRENT_TIMESTAMP()) <= MAX(threshold)
      THEN '🟡 Aging'
    ELSE '🔴 Stale'
  END AS freshness_status,
  ROUND(
    TIMESTAMPDIFF(MINUTE, MAX(checked_at), CURRENT_TIMESTAMP()) * 100.0 / NULLIF(MAX(threshold), 0),
    0
  ) AS sla_consumed_pct
FROM medallion_demo.gold.data_quality_metrics
WHERE check_type = 'freshness'
GROUP BY table_name
ORDER BY minutes_stale DESC;
