-- Read Elements From Data Source 'revenue'
-- Metric Time Dimension 'ds'
-- Constrain Time Range to [2000-01-01T00:00:00, 2020-01-01T00:00:00]
-- Pass Only Elements:
--   ['txn_revenue', 'ds__month']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(revenue) AS revenue_all_time
  , DATE_TRUNC(created_at, month) AS ds__month
FROM (
  -- User Defined SQL Query
  SELECT * FROM ***************************.fct_revenue
) revenue_src_10005
WHERE created_at BETWEEN CAST('2000-01-01' AS DATETIME) AND CAST('2020-01-01' AS DATETIME)
GROUP BY
  ds__month
