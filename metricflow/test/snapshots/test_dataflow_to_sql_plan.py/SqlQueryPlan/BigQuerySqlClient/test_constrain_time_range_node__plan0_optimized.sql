-- Read Elements From Data Source 'bookings_source'
-- Pass Only Elements:
--   ['bookings', 'ds']
-- Metric Time Dimension 'ds'
-- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-02T00:00:00]
SELECT
  ds
  , ds AS metric_time
  , 1 AS bookings
FROM (
  -- User Defined SQL Query
  SELECT * FROM ***************************.fct_bookings
) bookings_source_src_10000
WHERE ds BETWEEN CAST('2020-01-01' AS DATETIME) AND CAST('2020-01-02' AS DATETIME)
