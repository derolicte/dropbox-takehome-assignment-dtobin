-- This SQL script calculates daily pacing metrics for Q2 2025
-- It aggregates bookings, forecasts, and targets by day, region, product, and channel type
DROP TABLE IF EXISTS dbx.q2_pacing_by_day;
CREATE TABLE dbx.q2_pacing_by_day AS
WITH calendar AS (
  SELECT generate_series(DATE '2025-04-01', DATE '2025-06-30', INTERVAL '1 day')::date AS calendar_day
),

q2_bookings AS (
  SELECT 
    DATE(close_date) AS close_date,
    reporting_region AS region,
    product_flag AS product,
    deal_type AS channel_type,
    net_new_acv_usd
  FROM dbx.bookings
  WHERE DATE(close_date) BETWEEN '2025-04-01' AND '2025-06-30'
),
-- All unique combinations of dimensions across the quarter
unique_dim AS (
  SELECT DISTINCT 
    reporting_region AS region,
    product_flag AS product,
    deal_type AS channel_type
  FROM dbx.bookings
  WHERE DATE(close_date) BETWEEN '2025-04-01' AND '2025-06-30'
),

-- Prepare date x dimension grid
date_dim_cross AS (
  SELECT 
    c.calendar_day,
    u.region,
    u.product,
    u.channel_type
  FROM calendar c
  CROSS JOIN unique_dim u
),

-- Join actual bookings to daily grid
daily_bookings AS (
  SELECT 
    d.calendar_day,
    d.region,
    d.product,
    d.channel_type,
    COALESCE(SUM(b.net_new_acv_usd), 0) AS daily_acv
  FROM date_dim_cross d
  LEFT JOIN q2_bookings b
    ON DATE(b.close_date) = d.calendar_day
    AND b.region = d.region
    AND b.product = d.product
    AND b.channel_type = d.channel_type
  GROUP BY d.calendar_day, d.region, d.product, d.channel_type
),
-- Max booking date per combo
last_booking_date AS (
  SELECT 
    region,
    product,
    channel_type,
    MAX(calendar_day) AS last_close_date
  FROM daily_bookings
  WHERE daily_acv > 0
  GROUP BY region, product, channel_type
),
-- Add forecast (final expected ACV for the quarter)
forecast_values AS (
  SELECT 
    region,
    product,
    -- We do not currently have forecast data for 'Channel'
    'Direct' AS channel_type,
    ROUND(SUM(forecast_value::NUMERIC) / 91, 3) AS forecast_value -- Average daily forecast for the quarter
  FROM dbx.forecast
  WHERE LOWER(product) != 'total'  -- exclude rollups
    AND level = 'Manager'  -- only use manager-level forecasts
  GROUP BY 1, 2, 3
),

-- Add targets (filter GLOBAL separately if needed)
target_values AS (
  SELECT 
    region,
    product,
    type AS channel_type,
    ROUND(SUM(net_new_acv_target::NUMERIC / 91), 3) AS target_value
  FROM dbx.targets
  WHERE LOWER(type) != 'total' AND LOWER(region) != 'global'
  GROUP BY 1, 2, 3
),

-- Combine pacing with forecast + targets
pacing_with_meta AS (
  SELECT 
    b.*,
    f.forecast_value,
    t.target_value
  FROM daily_bookings b
  LEFT JOIN forecast_values f
    ON b.region = f.region AND b.product = f.product AND b.channel_type = f.channel_type
  LEFT JOIN target_values t
    ON b.region = t.region AND b.product = t.product AND b.channel_type = t.channel_type
)

-- Final pacing metrics
SELECT 
  p.*,
  CASE 
    WHEN p.calendar_day <= l.last_close_date THEN 
      SUM(daily_acv) OVER (PARTITION BY p.region, p.product, p.channel_type ORDER BY p.calendar_day)
    ELSE NULL
  END AS cumulative_acv,
  ROW_NUMBER() OVER (PARTITION BY p.region, p.product, p.channel_type ORDER BY p.calendar_day) AS pacing_day,
  ROUND(
    ROW_NUMBER() OVER (PARTITION BY p.region, p.product, p.channel_type ORDER BY p.calendar_day)::NUMERIC 
    / COUNT(*) OVER (PARTITION BY p.region, p.product, p.channel_type),
    3
  ) AS pct_qtr_elapsed
FROM pacing_with_meta p
LEFT JOIN last_booking_date l
  ON p.region = l.region AND p.product = l.product AND p.channel_type = l.channel_type
ORDER BY 
  p.region, 
  p.product, 
  p.channel_type, 
  p.calendar_day;