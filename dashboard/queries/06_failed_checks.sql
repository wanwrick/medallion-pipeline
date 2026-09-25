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
    TIMESTAMPDIFF(HOUR, MIN(checked_at), CURRENT_TIMESTAMP()) AS hours_open,
    MAX_BY(details, checked_at) AS latest_detail,
    MAX_BY(metric_value, checked_at) AS latest_value,
    MAX_BY(threshold, checked_at) AS threshold
  FROM failures
  GROUP BY table_name, check_type
),

triaged AS (
  SELECT
    *,
    ROUND(ABS(latest_value - threshold) * 100.0 / NULLIF(threshold, 0), 1) AS pct_off_threshold,
    -- One rank drives both the label and the sort, so the top row is always
    -- the one its label says to work first. A first failure is noise until it
    -- repeats. A failure open for three days is not a quality problem any
    -- more, it is an ownership problem, however many times it has fired.
    CASE
      WHEN hours_open >= 72 THEN 1
      WHEN consecutive_failures >= 3 THEN 2
      WHEN consecutive_failures > 1 THEN 3
      ELSE 4
    END AS triage_rank
  FROM streaks
)

SELECT
  table_name,
  check_type,
  latest_value,
  threshold,
  pct_off_threshold,
  consecutive_failures,
  failing_since,
  last_seen,
  hours_open,
  CASE triage_rank
    WHEN 1 THEN '🔴 Stale, unowned'
    WHEN 2 THEN '🟠 Persistent'
    WHEN 3 THEN '🟡 Repeating'
    ELSE '⚪ First occurrence'
  END AS triage,
  latest_detail
FROM triaged
ORDER BY triage_rank, pct_off_threshold DESC;
