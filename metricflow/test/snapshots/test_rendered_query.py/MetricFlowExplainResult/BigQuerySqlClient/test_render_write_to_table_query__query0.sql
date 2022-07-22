CREATE TABLE ***************************.test_table AS (
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    SUM(bookings) AS bookings
    , ds
  FROM (
    -- Read Elements From Data Source 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Constrain Time Range to [2000-01-01T00:00:00, 2040-12-31T00:00:00]
    -- Pass Only Elements:
    --   ['bookings', 'ds']
    SELECT
      ds
      , 1 AS bookings
    FROM (
      -- User Defined SQL Query
      SELECT * FROM ***************************.fct_bookings
    ) bookings_source_src_0
    WHERE ds BETWEEN CAST('2000-01-01' AS DATETIME) AND CAST('2040-12-31' AS DATETIME)
  ) subq_3
  GROUP BY
    ds
)
