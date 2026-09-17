-- =============================================================================
-- FAILED CHECKS — Detail Table for Triage
-- =============================================================================
-- Purpose: The bottom panel of the dashboard. Every other widget says something
--          is wrong; this one says which check, on which table, since when, and
--          whether it is new. Ordered so the top row is the one to work first.
-- Widget:  Detail table with row expansion
-- =============================================================================

WITH failures AS (
  SELECT
    table_name,
    check_type,
    COALESCE(details, 'no detail recorded') AS details,
    metric_value,
    threshold,
    checked_at
  FROM medallion_demo.gold.data_quality_metrics
  WHERE status = 'FAIL'
    AND checked_at >= CURRENT_DATE() - INTERVAL 7 DAY
),

streaks AS (
  SELECT
    table_name,
    check_type,
    COUNT(*) AS consecutive_failures,
    MIN(checked_at) AS failing_since,
    MAX(checked_at) AS last_seen,
    MAX_BY(details, checked_at) AS latest_detail,
    MAX_BY(metric_value, checked_at) AS latest_value,
    MAX_BY(threshold, checked_at) AS threshold
  FROM failures
  GROUP BY table_name, check_type
)

SELECT
  table_name,
  check_type,
  latest_value,
  threshold,
  ROUND(ABS(latest_value - threshold) * 100.0 / NULLIF(threshold, 0), 1) AS pct_off_threshold,
  consecutive_failures,
  failing_since,
  last_seen,
  TIMESTAMPDIFF(HOUR, failing_since, CURRENT_TIMESTAMP()) AS hours_open,
  -- A first failure is noise until it repeats. A week-old failure is not a
  -- quality problem any more, it is an ownership problem.
  CASE
    WHEN consecutive_failures = 1 THEN '⚪ First occurrence'
    WHEN TIMESTAMPDIFF(HOUR, failing_since, CURRENT_TIMESTAMP()) >= 72 THEN '🔴 Stale, unowned'
    WHEN consecutive_failures >= 3 THEN '🟠 Persistent'
    ELSE '🟡 Repeating'
  END AS triage,
  latest_detail
FROM streaks
ORDER BY
  CASE
    WHEN TIMESTAMPDIFF(HOUR, failing_since, CURRENT_TIMESTAMP()) >= 72 THEN 1
    WHEN consecutive_failures >= 3 THEN 2
    WHEN consecutive_failures > 1 THEN 3
    ELSE 4
  END,
  pct_off_threshold DESC;
