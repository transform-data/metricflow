-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(count_dogs) AS count_dogs
  , metric_time
FROM (
  -- Read Elements From Data Source 'animals'
  -- Metric Time Dimension 'ds'
  -- Constrain Time Range to [2000-01-01T00:00:00, 2040-12-31T00:00:00]
  -- Pass Only Elements:
  --   ['count_dogs', 'metric_time']
  SELECT
    ds AS metric_time
    , 1 AS count_dogs
  FROM (
    SELECT * FROM ***************************.fct_animals
  ) animals_src_0
  WHERE ds BETWEEN CAST('2000-01-01' AS TIMESTAMP) AND CAST('2040-12-31' AS TIMESTAMP)
) subq_3
GROUP BY
  metric_time
