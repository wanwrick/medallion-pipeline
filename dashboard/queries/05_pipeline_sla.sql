-- =============================================================================
-- PIPELINE SLA — Delivery Compliance Tracking
-- =============================================================================
-- Purpose: Report SLA compliance the way it gets asked about in a review:
--          how often did we hold the promise, and when we missed, by how much.
--          A mean is not an answer here. A single four-hour outage and forty
--          one-minute slips average the same and mean nothing alike, so this
--          reports the 95th percentile and the worst miss alongside the rate.
-- Widget:  Bar chart by table, plus a compliance trend line
-- =============================================================================

WITH runs AS (
  SELECT
    table_name,
    DATE(checked_at) AS run_date,
    metric_value AS observed_minutes,
    threshold AS sla_minutes,
    metric_value <= threshold AS within_sla
  FROM medallion_demo.gold.data_quality_metrics
  WHERE check_type = 'freshness'
    AND threshold IS NOT NULL
    AND checked_at >= CURRENT_DATE() - INTERVAL 30 DAY
)

SELECT
  table_name,
  COUNT(*) AS runs_observed,
  SUM(CASE WHEN within_sla THEN 1 ELSE 0 END) AS runs_within_sla,
  ROUND(
    SUM(CASE WHEN within_sla THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0),
    2
  ) AS sla_compliance_pct,
  MAX(sla_minutes) AS sla_minutes,
  ROUND(AVG(observed_minutes), 1) AS avg_minutes,
  ROUND(PERCENTILE(observed_minutes, 0.95), 1) AS p95_minutes,
  ROUND(MAX(observed_minutes), 1) AS worst_minutes,
  -- Error budget: how much of the allowed miss rate is already spent.
  -- 99% target over 30 days leaves room for roughly 7 hours of breach.
  ROUND(
    (COUNT(*) - SUM(CASE WHEN within_sla THEN 1 ELSE 0 END)) * 100.0
      / NULLIF(COUNT(*) * 0.01, 0),
    0
  ) AS error_budget_consumed_pct,
  CASE
    WHEN SUM(CASE WHEN within_sla THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) >= 99.0
      THEN '🟢 Met'
    WHEN SUM(CASE WHEN within_sla THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) >= 98.0
      THEN '🟡 At risk'
    ELSE '🔴 Breached'
  END AS sla_status
FROM runs
GROUP BY table_name
ORDER BY sla_compliance_pct ASC;
