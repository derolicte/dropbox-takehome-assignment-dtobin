-- This SQL script calculates the pipeline coverage for Q2 2025
-- It aggregates pipeline, bookings, and targets by region, product, and channel type
CREATE OR REPLACE VIEW dbx.q2_pipeline_coverage AS 
WITH q2_window AS (
  SELECT DATE '2025-04-01' AS q2_start, DATE '2025-06-30' AS q2_end
),
-- Define manager to channel type mapping
-- This is used to ensure we have a channel type for each manager
manager_to_channel_type AS (
  SELECT DISTINCT
    reporting_manager AS manager,
    deal_type AS channel_type
  FROM dbx.bookings
),
bookings_q2 AS (
  SELECT
    reporting_region AS region,
    product_flag AS product,
    deal_type AS channel_type,
    SUM(net_new_acv_usd) AS bookings
  FROM dbx.bookings, q2_window
  WHERE DATE(close_date) BETWEEN q2_start AND q2_end
  GROUP BY 1, 2, 3

  UNION ALL

  -- Create a global bookings roll-up
  SELECT
    'GLOBAL' AS region,
    product_flag AS product,
    deal_type AS channel_type,
    SUM(net_new_acv_usd) AS bookings
  FROM dbx.bookings, q2_window
  WHERE DATE(close_date) BETWEEN q2_start AND q2_end
  GROUP BY 2, 3

),
pipeline_now AS (
  SELECT
    pipe.reporting_region AS region,
    pipe.product_flag AS product,
    -- Use the manager to channel type mapping
    COALESCE(mgr_to_chnl.channel_type, 'UNKNOWN') AS channel_type,
    SUM(pipe.net_new_acv_usd) AS pipeline
  FROM dbx.pipeline pipe
  LEFT JOIN manager_to_channel_type mgr_to_chnl
    ON pipe.reporting_manager = mgr_to_chnl.manager
  WHERE net_new_acv_usd > 0
    AND DATE(pipe.close_date) BETWEEN DATE '2025-04-01' AND DATE '2025-06-30'
  GROUP BY 1, 2, 3

  UNION ALL

  -- Global pipeline aggregation
  SELECT
    'GLOBAL' AS region,
    pipe.product_flag AS product,
    COALESCE(mgr_to_chnl.channel_type, 'UNKNOWN') AS channel_type,
    SUM(pipe.net_new_acv_usd) AS pipeline
  FROM dbx.pipeline pipe
  LEFT JOIN manager_to_channel_type mgr_to_chnl
    ON pipe.reporting_manager = mgr_to_chnl.manager
  -- Ensure we only include valid pipeline entries
  WHERE net_new_acv_usd > 0
    AND DATE(pipe.close_date) BETWEEN DATE '2025-04-01' AND DATE '2025-06-30'
  GROUP BY 2, 3
),
targets AS (
  SELECT
    region,
    product,
    type AS channel_type,
    SUM(net_new_acv_target) AS target
  FROM dbx.targets
  -- Remove global and total roll-ups
  -- to prevent double counting
  WHERE region != 'GLOBAL'
    AND product != 'Total'
    AND type != 'Total'
  GROUP BY 1, 2, 3

  UNION ALL 

  -- Global targets roll-up
  SELECT
    'GLOBAL' AS region,
    product,
    type AS channel_type,
    SUM(net_new_acv_target) AS target
  FROM dbx.targets
  -- Remove global and total roll-ups
  -- to prevent double counting
  WHERE region = 'GLOBAL'
    AND product != 'Total'
    AND type != 'Total'
  GROUP BY 2, 3
),
pipeline_coverage AS (
  SELECT
    p.region,
    p.product,
    p.channel_type,
    p.pipeline,
    COALESCE(b.bookings, 0) AS bookings,
    COALESCE(t.target, 0) AS target,
    -- Calculate coverage ratio
    ROUND(p.pipeline::NUMERIC / NULLIF((t.target - COALESCE(b.bookings, 0)), 0), 2) AS coverage_ratio
  FROM pipeline_now p
  LEFT JOIN bookings_q2 b
    ON p.region = b.region AND p.product = b.product AND p.channel_type = b.channel_type
  LEFT JOIN targets t
    ON p.region = t.region AND p.product = t.product AND p.channel_type = t.channel_type
)
SELECT * FROM pipeline_coverage
ORDER BY region, product, channel_type;

SELECT * FROM dbx.q2_pipeline_coverage;