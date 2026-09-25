# Runbook: quality_score_drop

**Fires when** the governed daily quality score falls below 98% on two consecutive evaluations. The score is the share of checks passing, as defined in `mv_quality_score_daily`.

**Owner:** data-platform-oncall. **Severity:** critical.

## First five minutes

1. Open the Failed Checks panel (query 06). The top row is the one to work first: it sorts by how long the failure has been open, then by how far off threshold it is.
2. If every failure sits on one table, it is a pipeline problem. Check the last update for that table in the pipeline event log.
3. If the failures span tables and share a check type, it is a check problem. A threshold moved, or the source changed shape. Compare the details column against the previous day.
4. If `total_checks` fell along with the score, the checks job did not finish. Re-run the quality task before touching anything else.

## Resolve

- Fix forward in the pipeline. Never edit rows in `data_quality_metrics`.
- The alert resolves itself once the score clears 98% on the next evaluation.
- A failure still open after three days escalates to data-platform-lead through the `failure_unowned` alert. Do not wait for that.
