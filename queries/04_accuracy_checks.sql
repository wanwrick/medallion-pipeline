-- =============================================================================
-- ACCURACY CHECKS — Cross-Table Reconciliation
-- =============================================================================
-- Purpose: Completeness tells you a column is populated. Accuracy tells you the
--          value is right. These checks reconcile each layer against the one
--          above it, so a silently dropped batch shows up as a gap rather than
--          as a healthy-looking table.
-- Widget:  Table with conditional formatting, plus a single reconciliation KPI
-- =============================================================================

WITH reconciliation AS (
  SELECT
    table_name,
    MAX(CASE WHEN check_type = 'source_row_count' THEN metric_value END) AS source_rows,
    MAX(CASE WHEN check_type = 'row_count'        THEN metric_value END) AS landed_rows,
    MAX(CASE WHEN check_type = 'referential'      THEN metric_value END) AS orphan_rate,
    MAX(CASE WHEN check_type = 'uniqueness'       THEN metric_value END) AS uniqueness_pct,
    MAX(checked_at) AS checked_at
  FROM medallion_demo.gold.data_quality_metrics
  WHERE checked_at >= CURRENT_DATE() - INTERVAL 1 DAY
  GROUP BY table_name
),

scored AS (
  SELECT
    table_name,
    source_rows,
    landed_rows,
    source_rows - landed_rows AS row_variance,
    ROUND(
      ABS(source_rows - landed_rows) * 100.0 / NULLIF(source_rows, 0),
      3
    ) AS variance_pct,
    ROUND(COALESCE(orphan_rate, 0), 3) AS orphan_rate_pct,
    ROUND(COALESCE(uniqueness_pct, 100), 2) AS uniqueness_pct,
    checked_at
  FROM reconciliation
  WHERE source_rows IS NOT NULL
)

SELECT
  table_name,
  source_rows,
  landed_rows,
  row_variance,
  variance_pct,
  orphan_rate_pct,
  uniqueness_pct,
  -- A 0.1% variance tolerance absorbs in-flight records at the batch boundary.
  -- Anything wider is a real loss and needs a named owner, not a retry.
  CASE
    WHEN variance_pct <= 0.1 AND orphan_rate_pct <= 0.1 THEN '🟢 Reconciled'
    WHEN variance_pct <= 1.0 AND orphan_rate_pct <= 1.0 THEN '🟡 Drifting'
    ELSE '🔴 Breached'
  END AS accuracy_status,
  CASE
    WHEN variance_pct > 1.0  THEN 'Row loss between source and landed'
    WHEN orphan_rate_pct > 1.0 THEN 'Foreign keys with no matching dimension'
    WHEN uniqueness_pct < 99.0 THEN 'Duplicate business keys'
    ELSE 'Within tolerance'
  END AS finding,
  checked_at
FROM scored
ORDER BY variance_pct DESC, orphan_rate_pct DESC;
