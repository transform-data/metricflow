-- Compute Metrics via Expressions
SELECT
  booking_value * 0.05 AS booking_fees
  , listing
  , listing__country_latest
FROM (
  -- Join Standard Outputs
  -- Aggregate Measures
  SELECT
    bookings_source_src_10000.listing_id AS listing
    , listings_latest_src_10003.country AS listing__country_latest
    , SUM(bookings_source_src_10000.booking_value) AS booking_value
  FROM (
    -- User Defined SQL Query
    SELECT * FROM ***************************.fct_bookings
  ) bookings_source_src_10000
  LEFT OUTER JOIN
    ***************************.dim_listings_latest listings_latest_src_10003
  ON
    bookings_source_src_10000.listing_id = listings_latest_src_10003.listing_id
  GROUP BY
    listing
    , listing__country_latest
) subq_11
