-- Compute Metrics via Expressions
SELECT
  average_booking_value * bookings / NULLIF(booking_value, 0) AS lux_booking_value_rate_expr
  , subq_16.metric_time
FROM (
  -- Pass Only Elements:
  --   ['metric_time', 'average_booking_value', 'bookings', 'booking_value']
  SELECT
    subq_15.metric_time
    , subq_15.bookings
    , subq_15.average_booking_value
    , subq_15.booking_value
  FROM (
    -- Join Aggregated Measures with Standard Outputs
    SELECT
      subq_10.metric_time AS metric_time
      , subq_10.bookings AS bookings
      , subq_10.average_booking_value AS average_booking_value
      , subq_14.booking_value AS booking_value
    FROM (
      -- Aggregate Measures
      SELECT
        subq_9.metric_time
        , SUM(subq_9.bookings) AS bookings
        , AVG(subq_9.average_booking_value) AS average_booking_value
      FROM (
        -- Pass Only Elements:
        --   ['average_booking_value', 'bookings', 'metric_time']
        SELECT
          subq_8.metric_time
          , subq_8.bookings
          , subq_8.average_booking_value
        FROM (
          -- Constrain Output with WHERE
          SELECT
            subq_7.metric_time
            , subq_7.listing__is_lux_latest
            , subq_7.bookings
            , subq_7.average_booking_value
          FROM (
            -- Pass Only Elements:
            --   ['average_booking_value', 'bookings', 'listing__is_lux_latest', 'metric_time']
            SELECT
              subq_6.metric_time
              , subq_6.listing__is_lux_latest
              , subq_6.bookings
              , subq_6.average_booking_value
            FROM (
              -- Join Standard Outputs
              SELECT
                subq_2.metric_time AS metric_time
                , subq_2.listing AS listing
                , subq_5.is_lux_latest AS listing__is_lux_latest
                , subq_2.bookings AS bookings
                , subq_2.average_booking_value AS average_booking_value
              FROM (
                -- Pass Only Elements:
                --   ['average_booking_value', 'bookings', 'metric_time', 'listing']
                SELECT
                  subq_1.metric_time
                  , subq_1.listing
                  , subq_1.bookings
                  , subq_1.average_booking_value
                FROM (
                  -- Metric Time Dimension 'ds'
                  SELECT
                    subq_0.ds
                    , subq_0.ds__week
                    , subq_0.ds__month
                    , subq_0.ds__quarter
                    , subq_0.ds__year
                    , subq_0.ds_partitioned
                    , subq_0.ds_partitioned__week
                    , subq_0.ds_partitioned__month
                    , subq_0.ds_partitioned__quarter
                    , subq_0.ds_partitioned__year
                    , subq_0.booking_paid_at
                    , subq_0.booking_paid_at__week
                    , subq_0.booking_paid_at__month
                    , subq_0.booking_paid_at__quarter
                    , subq_0.booking_paid_at__year
                    , subq_0.create_a_cycle_in_the_join_graph__ds
                    , subq_0.create_a_cycle_in_the_join_graph__ds__week
                    , subq_0.create_a_cycle_in_the_join_graph__ds__month
                    , subq_0.create_a_cycle_in_the_join_graph__ds__quarter
                    , subq_0.create_a_cycle_in_the_join_graph__ds__year
                    , subq_0.create_a_cycle_in_the_join_graph__ds_partitioned
                    , subq_0.create_a_cycle_in_the_join_graph__ds_partitioned__week
                    , subq_0.create_a_cycle_in_the_join_graph__ds_partitioned__month
                    , subq_0.create_a_cycle_in_the_join_graph__ds_partitioned__quarter
                    , subq_0.create_a_cycle_in_the_join_graph__ds_partitioned__year
                    , subq_0.create_a_cycle_in_the_join_graph__booking_paid_at
                    , subq_0.create_a_cycle_in_the_join_graph__booking_paid_at__week
                    , subq_0.create_a_cycle_in_the_join_graph__booking_paid_at__month
                    , subq_0.create_a_cycle_in_the_join_graph__booking_paid_at__quarter
                    , subq_0.create_a_cycle_in_the_join_graph__booking_paid_at__year
                    , subq_0.ds AS metric_time
                    , subq_0.ds__week AS metric_time__week
                    , subq_0.ds__month AS metric_time__month
                    , subq_0.ds__quarter AS metric_time__quarter
                    , subq_0.ds__year AS metric_time__year
                    , subq_0.listing
                    , subq_0.guest
                    , subq_0.host
                    , subq_0.create_a_cycle_in_the_join_graph
                    , subq_0.create_a_cycle_in_the_join_graph__listing
                    , subq_0.create_a_cycle_in_the_join_graph__guest
                    , subq_0.create_a_cycle_in_the_join_graph__host
                    , subq_0.is_instant
                    , subq_0.create_a_cycle_in_the_join_graph__is_instant
                    , subq_0.bookings
                    , subq_0.instant_bookings
                    , subq_0.booking_value
                    , subq_0.max_booking_value
                    , subq_0.min_booking_value
                    , subq_0.bookers
                    , subq_0.average_booking_value
                  FROM (
                    -- Read Elements From Data Source 'bookings_source'
                    SELECT
                      1 AS bookings
                      , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
                      , bookings_source_src_10000.booking_value
                      , bookings_source_src_10000.booking_value AS max_booking_value
                      , bookings_source_src_10000.booking_value AS min_booking_value
                      , bookings_source_src_10000.guest_id AS bookers
                      , bookings_source_src_10000.booking_value AS average_booking_value
                      , bookings_source_src_10000.booking_value AS booking_payments
                      , bookings_source_src_10000.is_instant
                      , bookings_source_src_10000.ds
                      , DATE_TRUNC(bookings_source_src_10000.ds, isoweek) AS ds__week
                      , DATE_TRUNC(bookings_source_src_10000.ds, month) AS ds__month
                      , DATE_TRUNC(bookings_source_src_10000.ds, quarter) AS ds__quarter
                      , DATE_TRUNC(bookings_source_src_10000.ds, isoyear) AS ds__year
                      , bookings_source_src_10000.ds_partitioned
                      , DATE_TRUNC(bookings_source_src_10000.ds_partitioned, isoweek) AS ds_partitioned__week
                      , DATE_TRUNC(bookings_source_src_10000.ds_partitioned, month) AS ds_partitioned__month
                      , DATE_TRUNC(bookings_source_src_10000.ds_partitioned, quarter) AS ds_partitioned__quarter
                      , DATE_TRUNC(bookings_source_src_10000.ds_partitioned, isoyear) AS ds_partitioned__year
                      , bookings_source_src_10000.booking_paid_at
                      , DATE_TRUNC(bookings_source_src_10000.booking_paid_at, isoweek) AS booking_paid_at__week
                      , DATE_TRUNC(bookings_source_src_10000.booking_paid_at, month) AS booking_paid_at__month
                      , DATE_TRUNC(bookings_source_src_10000.booking_paid_at, quarter) AS booking_paid_at__quarter
                      , DATE_TRUNC(bookings_source_src_10000.booking_paid_at, isoyear) AS booking_paid_at__year
                      , bookings_source_src_10000.is_instant AS create_a_cycle_in_the_join_graph__is_instant
                      , bookings_source_src_10000.ds AS create_a_cycle_in_the_join_graph__ds
                      , DATE_TRUNC(bookings_source_src_10000.ds, isoweek) AS create_a_cycle_in_the_join_graph__ds__week
                      , DATE_TRUNC(bookings_source_src_10000.ds, month) AS create_a_cycle_in_the_join_graph__ds__month
                      , DATE_TRUNC(bookings_source_src_10000.ds, quarter) AS create_a_cycle_in_the_join_graph__ds__quarter
                      , DATE_TRUNC(bookings_source_src_10000.ds, isoyear) AS create_a_cycle_in_the_join_graph__ds__year
                      , bookings_source_src_10000.ds_partitioned AS create_a_cycle_in_the_join_graph__ds_partitioned
                      , DATE_TRUNC(bookings_source_src_10000.ds_partitioned, isoweek) AS create_a_cycle_in_the_join_graph__ds_partitioned__week
                      , DATE_TRUNC(bookings_source_src_10000.ds_partitioned, month) AS create_a_cycle_in_the_join_graph__ds_partitioned__month
                      , DATE_TRUNC(bookings_source_src_10000.ds_partitioned, quarter) AS create_a_cycle_in_the_join_graph__ds_partitioned__quarter
                      , DATE_TRUNC(bookings_source_src_10000.ds_partitioned, isoyear) AS create_a_cycle_in_the_join_graph__ds_partitioned__year
                      , bookings_source_src_10000.booking_paid_at AS create_a_cycle_in_the_join_graph__booking_paid_at
                      , DATE_TRUNC(bookings_source_src_10000.booking_paid_at, isoweek) AS create_a_cycle_in_the_join_graph__booking_paid_at__week
                      , DATE_TRUNC(bookings_source_src_10000.booking_paid_at, month) AS create_a_cycle_in_the_join_graph__booking_paid_at__month
                      , DATE_TRUNC(bookings_source_src_10000.booking_paid_at, quarter) AS create_a_cycle_in_the_join_graph__booking_paid_at__quarter
                      , DATE_TRUNC(bookings_source_src_10000.booking_paid_at, isoyear) AS create_a_cycle_in_the_join_graph__booking_paid_at__year
                      , bookings_source_src_10000.listing_id AS listing
                      , bookings_source_src_10000.guest_id AS guest
                      , bookings_source_src_10000.host_id AS host
                      , bookings_source_src_10000.guest_id AS create_a_cycle_in_the_join_graph
                      , bookings_source_src_10000.listing_id AS create_a_cycle_in_the_join_graph__listing
                      , bookings_source_src_10000.guest_id AS create_a_cycle_in_the_join_graph__guest
                      , bookings_source_src_10000.host_id AS create_a_cycle_in_the_join_graph__host
                    FROM (
                      -- User Defined SQL Query
                      SELECT * FROM ***************************.fct_bookings
                    ) bookings_source_src_10000
                  ) subq_0
                ) subq_1
              ) subq_2
              LEFT OUTER JOIN (
                -- Pass Only Elements:
                --   ['listing', 'is_lux_latest']
                SELECT
                  subq_4.listing
                  , subq_4.is_lux_latest
                FROM (
                  -- Metric Time Dimension 'ds'
                  SELECT
                    subq_3.ds
                    , subq_3.ds__week
                    , subq_3.ds__month
                    , subq_3.ds__quarter
                    , subq_3.ds__year
                    , subq_3.created_at
                    , subq_3.created_at__week
                    , subq_3.created_at__month
                    , subq_3.created_at__quarter
                    , subq_3.created_at__year
                    , subq_3.listing__ds
                    , subq_3.listing__ds__week
                    , subq_3.listing__ds__month
                    , subq_3.listing__ds__quarter
                    , subq_3.listing__ds__year
                    , subq_3.listing__created_at
                    , subq_3.listing__created_at__week
                    , subq_3.listing__created_at__month
                    , subq_3.listing__created_at__quarter
                    , subq_3.listing__created_at__year
                    , subq_3.ds AS metric_time
                    , subq_3.ds__week AS metric_time__week
                    , subq_3.ds__month AS metric_time__month
                    , subq_3.ds__quarter AS metric_time__quarter
                    , subq_3.ds__year AS metric_time__year
                    , subq_3.listing
                    , subq_3.user
                    , subq_3.listing__user
                    , subq_3.country_latest
                    , subq_3.is_lux_latest
                    , subq_3.capacity_latest
                    , subq_3.listing__country_latest
                    , subq_3.listing__is_lux_latest
                    , subq_3.listing__capacity_latest
                    , subq_3.listings
                    , subq_3.largest_listing
                    , subq_3.smallest_listing
                  FROM (
                    -- Read Elements From Data Source 'listings_latest'
                    SELECT
                      1 AS listings
                      , listings_latest_src_10003.capacity AS largest_listing
                      , listings_latest_src_10003.capacity AS smallest_listing
                      , listings_latest_src_10003.created_at AS ds
                      , DATE_TRUNC(listings_latest_src_10003.created_at, isoweek) AS ds__week
                      , DATE_TRUNC(listings_latest_src_10003.created_at, month) AS ds__month
                      , DATE_TRUNC(listings_latest_src_10003.created_at, quarter) AS ds__quarter
                      , DATE_TRUNC(listings_latest_src_10003.created_at, isoyear) AS ds__year
                      , listings_latest_src_10003.created_at
                      , DATE_TRUNC(listings_latest_src_10003.created_at, isoweek) AS created_at__week
                      , DATE_TRUNC(listings_latest_src_10003.created_at, month) AS created_at__month
                      , DATE_TRUNC(listings_latest_src_10003.created_at, quarter) AS created_at__quarter
                      , DATE_TRUNC(listings_latest_src_10003.created_at, isoyear) AS created_at__year
                      , listings_latest_src_10003.country AS country_latest
                      , listings_latest_src_10003.is_lux AS is_lux_latest
                      , listings_latest_src_10003.capacity AS capacity_latest
                      , listings_latest_src_10003.created_at AS listing__ds
                      , DATE_TRUNC(listings_latest_src_10003.created_at, isoweek) AS listing__ds__week
                      , DATE_TRUNC(listings_latest_src_10003.created_at, month) AS listing__ds__month
                      , DATE_TRUNC(listings_latest_src_10003.created_at, quarter) AS listing__ds__quarter
                      , DATE_TRUNC(listings_latest_src_10003.created_at, isoyear) AS listing__ds__year
                      , listings_latest_src_10003.created_at AS listing__created_at
                      , DATE_TRUNC(listings_latest_src_10003.created_at, isoweek) AS listing__created_at__week
                      , DATE_TRUNC(listings_latest_src_10003.created_at, month) AS listing__created_at__month
                      , DATE_TRUNC(listings_latest_src_10003.created_at, quarter) AS listing__created_at__quarter
                      , DATE_TRUNC(listings_latest_src_10003.created_at, isoyear) AS listing__created_at__year
                      , listings_latest_src_10003.country AS listing__country_latest
                      , listings_latest_src_10003.is_lux AS listing__is_lux_latest
                      , listings_latest_src_10003.capacity AS listing__capacity_latest
                      , listings_latest_src_10003.listing_id AS listing
                      , listings_latest_src_10003.user_id AS user
                      , listings_latest_src_10003.user_id AS listing__user
                    FROM ***************************.dim_listings_latest listings_latest_src_10003
                  ) subq_3
                ) subq_4
              ) subq_5
              ON
                subq_2.listing = subq_5.listing
            ) subq_6
          ) subq_7
          WHERE listing__is_lux_latest
        ) subq_8
      ) subq_9
      GROUP BY
        subq_9.metric_time
    ) subq_10
    INNER JOIN (
      -- Aggregate Measures
      SELECT
        subq_13.metric_time
        , SUM(subq_13.booking_value) AS booking_value
      FROM (
        -- Pass Only Elements:
        --   ['booking_value', 'metric_time']
        SELECT
          subq_12.metric_time
          , subq_12.booking_value
        FROM (
          -- Metric Time Dimension 'ds'
          SELECT
            subq_11.ds
            , subq_11.ds__week
            , subq_11.ds__month
            , subq_11.ds__quarter
            , subq_11.ds__year
            , subq_11.ds_partitioned
            , subq_11.ds_partitioned__week
            , subq_11.ds_partitioned__month
            , subq_11.ds_partitioned__quarter
            , subq_11.ds_partitioned__year
            , subq_11.booking_paid_at
            , subq_11.booking_paid_at__week
            , subq_11.booking_paid_at__month
            , subq_11.booking_paid_at__quarter
            , subq_11.booking_paid_at__year
            , subq_11.create_a_cycle_in_the_join_graph__ds
            , subq_11.create_a_cycle_in_the_join_graph__ds__week
            , subq_11.create_a_cycle_in_the_join_graph__ds__month
            , subq_11.create_a_cycle_in_the_join_graph__ds__quarter
            , subq_11.create_a_cycle_in_the_join_graph__ds__year
            , subq_11.create_a_cycle_in_the_join_graph__ds_partitioned
            , subq_11.create_a_cycle_in_the_join_graph__ds_partitioned__week
            , subq_11.create_a_cycle_in_the_join_graph__ds_partitioned__month
            , subq_11.create_a_cycle_in_the_join_graph__ds_partitioned__quarter
            , subq_11.create_a_cycle_in_the_join_graph__ds_partitioned__year
            , subq_11.create_a_cycle_in_the_join_graph__booking_paid_at
            , subq_11.create_a_cycle_in_the_join_graph__booking_paid_at__week
            , subq_11.create_a_cycle_in_the_join_graph__booking_paid_at__month
            , subq_11.create_a_cycle_in_the_join_graph__booking_paid_at__quarter
            , subq_11.create_a_cycle_in_the_join_graph__booking_paid_at__year
            , subq_11.ds AS metric_time
            , subq_11.ds__week AS metric_time__week
            , subq_11.ds__month AS metric_time__month
            , subq_11.ds__quarter AS metric_time__quarter
            , subq_11.ds__year AS metric_time__year
            , subq_11.listing
            , subq_11.guest
            , subq_11.host
            , subq_11.create_a_cycle_in_the_join_graph
            , subq_11.create_a_cycle_in_the_join_graph__listing
            , subq_11.create_a_cycle_in_the_join_graph__guest
            , subq_11.create_a_cycle_in_the_join_graph__host
            , subq_11.is_instant
            , subq_11.create_a_cycle_in_the_join_graph__is_instant
            , subq_11.bookings
            , subq_11.instant_bookings
            , subq_11.booking_value
            , subq_11.max_booking_value
            , subq_11.min_booking_value
            , subq_11.bookers
            , subq_11.average_booking_value
          FROM (
            -- Read Elements From Data Source 'bookings_source'
            SELECT
              1 AS bookings
              , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
              , bookings_source_src_10000.booking_value
              , bookings_source_src_10000.booking_value AS max_booking_value
              , bookings_source_src_10000.booking_value AS min_booking_value
              , bookings_source_src_10000.guest_id AS bookers
              , bookings_source_src_10000.booking_value AS average_booking_value
              , bookings_source_src_10000.booking_value AS booking_payments
              , bookings_source_src_10000.is_instant
              , bookings_source_src_10000.ds
              , DATE_TRUNC(bookings_source_src_10000.ds, isoweek) AS ds__week
              , DATE_TRUNC(bookings_source_src_10000.ds, month) AS ds__month
              , DATE_TRUNC(bookings_source_src_10000.ds, quarter) AS ds__quarter
              , DATE_TRUNC(bookings_source_src_10000.ds, isoyear) AS ds__year
              , bookings_source_src_10000.ds_partitioned
              , DATE_TRUNC(bookings_source_src_10000.ds_partitioned, isoweek) AS ds_partitioned__week
              , DATE_TRUNC(bookings_source_src_10000.ds_partitioned, month) AS ds_partitioned__month
              , DATE_TRUNC(bookings_source_src_10000.ds_partitioned, quarter) AS ds_partitioned__quarter
              , DATE_TRUNC(bookings_source_src_10000.ds_partitioned, isoyear) AS ds_partitioned__year
              , bookings_source_src_10000.booking_paid_at
              , DATE_TRUNC(bookings_source_src_10000.booking_paid_at, isoweek) AS booking_paid_at__week
              , DATE_TRUNC(bookings_source_src_10000.booking_paid_at, month) AS booking_paid_at__month
              , DATE_TRUNC(bookings_source_src_10000.booking_paid_at, quarter) AS booking_paid_at__quarter
              , DATE_TRUNC(bookings_source_src_10000.booking_paid_at, isoyear) AS booking_paid_at__year
              , bookings_source_src_10000.is_instant AS create_a_cycle_in_the_join_graph__is_instant
              , bookings_source_src_10000.ds AS create_a_cycle_in_the_join_graph__ds
              , DATE_TRUNC(bookings_source_src_10000.ds, isoweek) AS create_a_cycle_in_the_join_graph__ds__week
              , DATE_TRUNC(bookings_source_src_10000.ds, month) AS create_a_cycle_in_the_join_graph__ds__month
              , DATE_TRUNC(bookings_source_src_10000.ds, quarter) AS create_a_cycle_in_the_join_graph__ds__quarter
              , DATE_TRUNC(bookings_source_src_10000.ds, isoyear) AS create_a_cycle_in_the_join_graph__ds__year
              , bookings_source_src_10000.ds_partitioned AS create_a_cycle_in_the_join_graph__ds_partitioned
              , DATE_TRUNC(bookings_source_src_10000.ds_partitioned, isoweek) AS create_a_cycle_in_the_join_graph__ds_partitioned__week
              , DATE_TRUNC(bookings_source_src_10000.ds_partitioned, month) AS create_a_cycle_in_the_join_graph__ds_partitioned__month
              , DATE_TRUNC(bookings_source_src_10000.ds_partitioned, quarter) AS create_a_cycle_in_the_join_graph__ds_partitioned__quarter
              , DATE_TRUNC(bookings_source_src_10000.ds_partitioned, isoyear) AS create_a_cycle_in_the_join_graph__ds_partitioned__year
              , bookings_source_src_10000.booking_paid_at AS create_a_cycle_in_the_join_graph__booking_paid_at
              , DATE_TRUNC(bookings_source_src_10000.booking_paid_at, isoweek) AS create_a_cycle_in_the_join_graph__booking_paid_at__week
              , DATE_TRUNC(bookings_source_src_10000.booking_paid_at, month) AS create_a_cycle_in_the_join_graph__booking_paid_at__month
              , DATE_TRUNC(bookings_source_src_10000.booking_paid_at, quarter) AS create_a_cycle_in_the_join_graph__booking_paid_at__quarter
              , DATE_TRUNC(bookings_source_src_10000.booking_paid_at, isoyear) AS create_a_cycle_in_the_join_graph__booking_paid_at__year
              , bookings_source_src_10000.listing_id AS listing
              , bookings_source_src_10000.guest_id AS guest
              , bookings_source_src_10000.host_id AS host
              , bookings_source_src_10000.guest_id AS create_a_cycle_in_the_join_graph
              , bookings_source_src_10000.listing_id AS create_a_cycle_in_the_join_graph__listing
              , bookings_source_src_10000.guest_id AS create_a_cycle_in_the_join_graph__guest
              , bookings_source_src_10000.host_id AS create_a_cycle_in_the_join_graph__host
            FROM (
              -- User Defined SQL Query
              SELECT * FROM ***************************.fct_bookings
            ) bookings_source_src_10000
          ) subq_11
        ) subq_12
      ) subq_13
      GROUP BY
        subq_13.metric_time
    ) subq_14
    ON
      (
        (
          subq_10.metric_time = subq_14.metric_time
        ) OR (
          (subq_10.metric_time IS NULL) AND (subq_14.metric_time IS NULL)
        )
      )
  ) subq_15
) subq_16
