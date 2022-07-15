-- Compute Metrics via Expressions
SELECT
  average_booking_value * bookings / NULLIF(booking_value, 0) AS lux_booking_value_rate_expr
  , metric_time
FROM (
  -- Join Aggregated Measures with Standard Outputs
  -- Pass Only Elements:
  --   ['metric_time', 'average_booking_value', 'bookings', 'booking_value']
  SELECT
    subq_27.metric_time AS metric_time
    , subq_27.bookings AS bookings
    , subq_27.average_booking_value AS average_booking_value
    , subq_31.booking_value AS booking_value
  FROM (
    -- Constrain Output with WHERE
    -- Pass Only Elements:
    --   ['average_booking_value', 'bookings', 'metric_time']
    -- Aggregate Measures
    SELECT
      metric_time
      , SUM(bookings) AS bookings
      , AVG(average_booking_value) AS average_booking_value
    FROM (
      -- Join Standard Outputs
      -- Pass Only Elements:
      --   ['average_booking_value', 'bookings', 'listing__is_lux_latest', 'metric_time']
      SELECT
        subq_19.metric_time AS metric_time
        , listings_latest_src_10003.is_lux AS listing__is_lux_latest
        , subq_19.bookings AS bookings
        , subq_19.average_booking_value AS average_booking_value
      FROM (
        -- Read Elements From Data Source 'bookings_source'
        -- Metric Time Dimension 'ds'
        -- Pass Only Elements:
        --   ['average_booking_value', 'bookings', 'metric_time', 'listing']
        SELECT
          ds AS metric_time
          , listing_id AS listing
          , 1 AS bookings
          , booking_value AS average_booking_value
        FROM (
          -- User Defined SQL Query
          SELECT * FROM ***************************.fct_bookings
        ) bookings_source_src_10000
      ) subq_19
      LEFT OUTER JOIN
        ***************************.dim_listings_latest listings_latest_src_10003
      ON
        subq_19.listing = listings_latest_src_10003.listing_id
    ) subq_24
    WHERE listing__is_lux_latest
    GROUP BY
      metric_time
  ) subq_27
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
      metric_time
  ) subq_31
  ON
    (
      (
        subq_27.metric_time = subq_31.metric_time
      ) OR (
        (subq_27.metric_time IS NULL) AND (subq_31.metric_time IS NULL)
      )
    )
) subq_33
