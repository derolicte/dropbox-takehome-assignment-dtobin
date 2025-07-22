-- This SQL script calculates the forecast attainment for Q2 2025
-- It aggregates forecast values, targets, and calculates attainment ratios by region, product, and channel typ
-- Please note that we do not currently have forecast data for 'Channel'
CREATE OR REPLACE VIEW dbx.q2_forecast_attainment AS
WITH manager_to_channel_type AS (
  SELECT DISTINCT
    bk.reporting_manager AS manager,
    bk.deal_type AS channel_type
  FROM dbx.bookings bk
),
forecast_values AS (
  SELECT
    fct.region,
    fct.product,
    -- We do not currently have forecast data for 'Channel'
    COALESCE(mgr.channel_type, 'Direct') AS channel_type,
    SUM(fct.forecast_value) AS forecast
  FROM dbx.forecast fct
  LEFT JOIN manager_to_channel_type mgr
    ON fct.manager = mgr.manager
  WHERE fct.product IS NOT NULL AND LOWER(fct.product) != 'total'
    AND level = 'Manager'
    AND fct.region IN ('AMER', 'EMEA', 'APAC', 'JAPAN', 'DASH')
  GROUP BY 1, 2, 3

  UNION ALL

  -- Global forecast roll-up
  SELECT
    'GLOBAL' AS region,
    fct.product,
    -- We do not currently have forecast data for 'Channel'
    COALESCE(mgr.channel_type, 'Direct') AS channel_type,
    SUM(fct.forecast_value) AS forecast
  FROM dbx.forecast fct
  LEFT JOIN manager_to_channel_type mgr
    ON fct.manager = mgr.manager
  WHERE fct.product IS NOT NULL AND LOWER(fct.product) != 'total'
    AND level = 'Manager'
    AND fct.region IN ('AMER', 'EMEA', 'APAC', 'JAPAN', 'DASH')
  GROUP BY 1, 2, 3  
), 
targets_cleaned AS (
  SELECT
    tgt.region,
    tgt.product,
    type AS channel_type,
    SUM(tgt.net_new_acv_target) AS target
  FROM dbx.targets tgt
  WHERE tgt.product IS NOT NULL AND LOWER(tgt.product) != 'total'
    AND region != 'GLOBAL'
  GROUP BY 1, 2, 3

  UNION ALL

  -- Global targets roll-up
  SELECT
    'GLOBAL' AS region,
    tgt.product,
    type AS channel_type,
    SUM(tgt.net_new_acv_target) AS target
  FROM dbx.targets tgt
  WHERE tgt.product IS NOT NULL AND LOWER(tgt.product) != 'total'
    AND region != 'GLOBAL'
  GROUP BY 1, 2, 3
),
attainment AS (
  SELECT
    fv.region,
    fv.product,
    fv.channel_type,
    fv.forecast,
    tc.target,
    ROUND(fv.forecast::NUMERIC / NULLIF(tc.target, 0), 2) AS attainment_ratio
  FROM forecast_values fv
  LEFT JOIN targets_cleaned tc
    ON fv.region = tc.region AND fv.product = tc.product AND fv.channel_type = tc.channel_type
)
SELECT * FROM attainment
ORDER BY 1, 2, 3;
;

/*
  SELECT
    fct.region,
    fct.product,
    -- We do not currently have forecast data for 'Channel'
    'Direct'AS channel_type,
    SUM(fct.forecast_value) AS forecast
  FROM dbx.forecast fct

  WHERE fct.product IS NOT NULL AND LOWER(fct.product) != 'total'
    AND level = 'Manager'
    AND fct.region IN ('AMER', 'EMEA', 'APAC', 'JAPAN', 'DASH')
  GROUP BY 1, 2, 3

SELECT * FROM dbx.targets WHERE region = 'AMER' AND product = 'Core' AND type = 'Direct';
*/


SELECT * FROM dbx.q2_forecast_attainment