SELECT
  t.*,

  -- 1) CO2 kg = trip_distance * co2_grams_per_mile / 1000
  t.trip_distance * (
    SELECT co2_grams_per_mile 
    FROM vehicle_emissions 
    WHERE vehicle_type = 'green_taxi'
    LIMIT 1
  ) / 1000.0 AS trip_co2_kgs,

  -- 2) avg mph = trip_distance / trip_duration
  CASE
    WHEN date_diff('second', lpep_pickup_datetime, lpep_dropoff_datetime) = 0 
        THEN NULL
    ELSE t.trip_distance / (date_diff('second', lpep_pickup_datetime, lpep_dropoff_datetime) / 3600.0)  -- trip_duration in hours
  END AS avg_mph,

  -- 3-6) time parts from pickup
  EXTRACT(HOUR FROM lpep_pickup_datetime) AS hour_of_day,
  EXTRACT(DOW FROM lpep_pickup_datetime) AS day_of_week, -- 0=Sun,...,6=Sat
  EXTRACT(WEEK FROM lpep_pickup_datetime) AS week_of_year,
  EXTRACT(MONTH FROM lpep_pickup_datetime) AS month_of_year

FROM green_trips_2024 AS t