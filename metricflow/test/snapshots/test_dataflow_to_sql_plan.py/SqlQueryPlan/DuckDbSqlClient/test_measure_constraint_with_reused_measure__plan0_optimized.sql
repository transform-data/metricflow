-- Join Aggregated Measures with Standard Outputs
-- Pass Only Elements:
--   ['metric_time', 'booking_value_with_is_instant_constraint', 'booking_value']
-- Compute Metrics via Expressions
SELECT
  CAST(subq_17.booking_value_with_is_instant_constraint AS DOUBLE) / CAST(NULLIF(subq_21.booking_value, 0) AS DOUBLE) AS instant_booking_value_ratio
  , subq_17.metric_time AS metric_time
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements:
  --   ['booking_value', 'metric_time']
  -- Aggregate Measures
  SELECT
    metric_time
    , SUM(booking_value) AS booking_value_with_is_instant_constraint
  FROM (
    -- Read Elements From Data Source 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['booking_value', 'is_instant', 'metric_time']
    SELECT
      ds AS metric_time
      , is_instant
      , booking_value
    FROM (
      -- User Defined SQL Query
      SELECT * FROM ***************************.fct_bookings
    ) bookings_source_src_10000
  ) subq_14
  WHERE is_instant
  GROUP BY
    metric_time
) subq_17
INNER JOIN (
  -- Read Elements From Data Source 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements:
  --   ['booking_value', 'metric_time']
  -- Aggregate Measures
  SELECT
    ds AS metric_time
    , SUM(booking_value) AS booking_value
  FROM (
    -- User Defined SQL Query
    SELECT * FROM ***************************.fct_bookings
  ) bookings_source_src_10000
  GROUP BY
    ds
) subq_21
ON
  (
    (
      subq_17.metric_time = subq_21.metric_time
    ) OR (
      (subq_17.metric_time IS NULL) AND (subq_21.metric_time IS NULL)
    )
  )
