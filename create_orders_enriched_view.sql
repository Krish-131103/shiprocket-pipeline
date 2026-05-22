-- ============================================================
-- orders_enriched VIEW (consolidated — all fields)
-- ============================================================
-- Run this in BigQuery to create/replace the enriched view.
-- To add a new field in future: edit ONLY this file and re-run.
--
-- Tables used:
--   shiprocket.orders       → raw order data
--   shiprocket.zone_mapping → pincode → zone reference
--   shiprocket.ndr_data     → NDR attempt details
-- ============================================================

CREATE OR REPLACE VIEW `shiprocket-496417.shiprocket.orders_enriched` AS

-- Step 1: Deduplicate orders (API occasionally returns same order_id twice)
WITH deduped AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY awb_assign_date DESC, created_at DESC
    ) AS _row_num
  FROM `shiprocket-496417.shiprocket.orders`
),

unique_orders AS (
  SELECT * EXCEPT(_row_num)
  FROM deduped
  WHERE _row_num = 1
),

-- Step 2: Deduplicate zone_mapping on pickup + customer pincode only
zone_deduped AS (
  SELECT
    pickup_pincode,
    customer_pincode,
    ANY_VALUE(zone) AS zone
  FROM `shiprocket-496417.shiprocket.zone_mapping`
  WHERE courier NOT IN ('Bihar', 'Haryana', 'Ladakh', 'Odisha')
  GROUP BY pickup_pincode, customer_pincode
)

SELECT
  -- ── All raw order fields ────────────────────────────────────────────
  o.*,

  -- ── Warehouse ──────────────────────────────────────────────────────
  CASE o.pickup_pincode
    WHEN '122505' THEN 'GGN'
    WHEN '421302' THEN 'MUM'
    WHEN '562123' THEN 'BLR'
    ELSE 'Unknown'
  END AS warehouse,

  -- ── Zone ───────────────────────────────────────────────────────────
  zm.zone AS mapped_zone,

  -- ── Weight category ────────────────────────────────────────────────
  CASE
    WHEN o.weight_kg IS NULL         THEN 'Unknown'
    WHEN o.weight_kg * 1000 <= 300   THEN '<=300g'
    WHEN o.weight_kg * 1000 <= 600   THEN '301-600g'
    WHEN o.weight_kg * 1000 <= 900   THEN '601-900g'
    WHEN o.weight_kg * 1000 <= 1200  THEN '901-1200g'
    WHEN o.weight_kg * 1000 <= 1500  THEN '1201-1500g'
    WHEN o.weight_kg * 1000 <= 1800  THEN '1501-1800g'
    WHEN o.weight_kg * 1000 <= 2100  THEN '1801-2100g'
    ELSE '2100g+'
  END AS weight_category,

  -- ── RTO flag ───────────────────────────────────────────────────────
  CASE
    WHEN UPPER(TRIM(o.status)) IN (
      'RTO INITIATED',
      'RTO IN TRANSIT',
      'RTO OFD',
      'RETURN CANCELLED',
      'RETURN IN TRANSIT',
      'RTO DELIVERED',
      'RTO NDR',
      'RETURN DELIVERED'
    ) THEN 'RTO'
    ELSE 'Non RTO'
  END AS rto,

  -- ── Extracted from products_json ───────────────────────────────────
  JSON_EXTRACT_SCALAR(o.products_json, '$[0].channel_sku') AS channel_sku,

  SAFE_CAST(
    JSON_EXTRACT_SCALAR(o.products_json, '$[0].tax_percentage')
  AS FLOAT64) AS tax_pct,

  (
    SELECT STRING_AGG(
      JSON_EXTRACT_SCALAR(p, '$.channel_sku'), ' | '
      ORDER BY JSON_EXTRACT_SCALAR(p, '$.channel_sku')
    )
    FROM UNNEST(JSON_EXTRACT_ARRAY(o.products_json)) AS p
    WHERE JSON_EXTRACT_SCALAR(p, '$.channel_sku') IS NOT NULL
  ) AS all_skus,

  -- ── Extracted from shipments_json ──────────────────────────────────
  JSON_EXTRACT_SCALAR(o.shipments_json, '$[0].sub_status')
    AS sub_status,

  JSON_EXTRACT_SCALAR(o.shipments_json, '$[0].pickedup_timestamp')
    AS pickedup_timestamp,

  JSON_EXTRACT_SCALAR(o.shipments_json, '$[0].sr_courier_name')
    AS master_courier,

  JSON_EXTRACT_SCALAR(o.shipments_json, '$[0].weight_action')
    AS charged_weight_action,

  -- ── Fulfillment TAT (days from order to delivery) ──────────────────
  -- delivered_date format : 'DD-MM-YYYY HH:MM:SS'
  -- channel_created_at format: 'D Mon YYYY, HH:MM AM/PM'
  DATE_DIFF(
    PARSE_DATE('%d-%m-%Y',
      REGEXP_EXTRACT(o.delivered_date, r'^(\d{2}-\d{2}-\d{4})')),
    PARSE_DATE('%d %b %Y',
      REGEXP_EXTRACT(o.channel_created_at, r'^\d{1,2} \w+ \d{4}')),
    DAY
  ) AS fulfillment_tat_days,

  -- ── NDR fields (joined from ndr_data table on AWB) ─────────────────
  ndr.reason             AS ndr_reason,
  ndr.total_ndr_attempts,
  ndr.ndr_raised_at,
  ndr.escalation_status  AS ndr_escalation_status,
  ndr.ndr_1_date,
  ndr.ndr_1_reason,
  ndr.ndr_1_comment,
  ndr.ndr_1_sms_response,
  ndr.ndr_2_date,
  ndr.ndr_2_reason,
  ndr.ndr_2_comment,
  ndr.ndr_2_sms_response,
  ndr.ndr_3_date,
  ndr.ndr_3_reason,
  ndr.ndr_3_comment,
  ndr.ndr_3_sms_response,
  ndr.latest_ndr_date,
  ndr.latest_ndr_reason,

  -- ── First Attempt Date (FAD) ────────────────────────────────────────
  -- For NDR orders  → ndr_1_date
  -- For delivered   → out_for_delivery_date (if available)
  -- NULL if neither is available
  COALESCE(
    SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', ndr.ndr_1_date),
    SAFE.PARSE_DATETIME('%d-%m-%Y %H:%M:%S', o.delivered_date)
  ) AS first_attempt_datetime,

  -- ── 1. Order Placed → Picked Up ────────────────────────────────────
  -- NULL if picked_up_date is NULL
  ROUND(
    TIMESTAMP_DIFF(
      CAST(SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', o.picked_up_date) AS TIMESTAMP),
      CAST(SAFE.PARSE_DATETIME('%e %b %Y, %I:%M %p',
        REGEXP_REPLACE(o.channel_created_at, r'\b(\d)\b', r'0\1')) AS TIMESTAMP),
      MINUTE
    ) / 1440.0,
  3) AS placed_to_pickup_days,

  CONCAT(
    CAST(TIMESTAMP_DIFF(
      CAST(SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', o.picked_up_date) AS TIMESTAMP),
      CAST(SAFE.PARSE_DATETIME('%e %b %Y, %I:%M %p',
        REGEXP_REPLACE(o.channel_created_at, r'\b(\d)\b', r'0\1')) AS TIMESTAMP),
      HOUR
    ) AS STRING), ' hrs'
  ) AS placed_to_pickup_hrs,

  -- ── 2. Picked Up → First Attempt Delivery ──────────────────────────
  -- NULL if picked_up_date or FAD is NULL
  ROUND(
    TIMESTAMP_DIFF(
      CAST(COALESCE(
        SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', ndr.ndr_1_date),
        SAFE.PARSE_DATETIME('%d-%m-%Y %H:%M:%S', o.delivered_date)
      ) AS TIMESTAMP),
      CAST(SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', o.picked_up_date) AS TIMESTAMP),
      MINUTE
    ) / 1440.0,
  3) AS pickup_to_fad_days,

  CONCAT(
    CAST(TIMESTAMP_DIFF(
      CAST(COALESCE(
        SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', ndr.ndr_1_date),
        SAFE.PARSE_DATETIME('%d-%m-%Y %H:%M:%S', o.delivered_date)
      ) AS TIMESTAMP),
      CAST(SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', o.picked_up_date) AS TIMESTAMP),
      HOUR
    ) AS STRING), ' hrs'
  ) AS pickup_to_fad_hrs,

  -- ── 3. Order Placed → First Attempt Delivery ───────────────────────
  -- NULL if FAD is NULL
  ROUND(
    TIMESTAMP_DIFF(
      CAST(COALESCE(
        SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', ndr.ndr_1_date),
        SAFE.PARSE_DATETIME('%d-%m-%Y %H:%M:%S', o.delivered_date)
      ) AS TIMESTAMP),
      CAST(SAFE.PARSE_DATETIME('%e %b %Y, %I:%M %p',
        REGEXP_REPLACE(o.channel_created_at, r'\b(\d)\b', r'0\1')) AS TIMESTAMP),
      MINUTE
    ) / 1440.0,
  3) AS placed_to_fad_days,

  CONCAT(
    CAST(TIMESTAMP_DIFF(
      CAST(COALESCE(
        SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', ndr.ndr_1_date),
        SAFE.PARSE_DATETIME('%d-%m-%Y %H:%M:%S', o.delivered_date)
      ) AS TIMESTAMP),
      CAST(SAFE.PARSE_DATETIME('%e %b %Y, %I:%M %p',
        REGEXP_REPLACE(o.channel_created_at, r'\b(\d)\b', r'0\1')) AS TIMESTAMP),
      HOUR
    ) AS STRING), ' hrs'
  ) AS placed_to_fad_hrs,

  -- ── 4. Order Placed → Delivered ────────────────────────────────────
  -- NULL if delivered_date is NULL
  ROUND(
    TIMESTAMP_DIFF(
      CAST(SAFE.PARSE_DATETIME('%d-%m-%Y %H:%M:%S', o.delivered_date) AS TIMESTAMP),
      CAST(SAFE.PARSE_DATETIME('%e %b %Y, %I:%M %p',
        REGEXP_REPLACE(o.channel_created_at, r'\b(\d)\b', r'0\1')) AS TIMESTAMP),
      MINUTE
    ) / 1440.0,
  3) AS placed_to_delivered_days,

  CONCAT(
    CAST(TIMESTAMP_DIFF(
      CAST(SAFE.PARSE_DATETIME('%d-%m-%Y %H:%M:%S', o.delivered_date) AS TIMESTAMP),
      CAST(SAFE.PARSE_DATETIME('%e %b %Y, %I:%M %p',
        REGEXP_REPLACE(o.channel_created_at, r'\b(\d)\b', r'0\1')) AS TIMESTAMP),
      HOUR
    ) AS STRING), ' hrs'
  ) AS placed_to_delivered_hrs,

  -- ── 5. Next Day Delivery (NDD) flag ────────────────────────────────
  -- NULL if delivered_date is NULL (order not yet delivered)
  CASE
    WHEN o.delivered_date IS NULL OR o.delivered_date = '' THEN NULL
    WHEN DATE_DIFF(
      PARSE_DATE('%d-%m-%Y',
        REGEXP_EXTRACT(o.delivered_date, r'^(\d{2}-\d{2}-\d{4})')),
      PARSE_DATE('%d %b %Y',
        REGEXP_EXTRACT(o.channel_created_at, r'^\d{1,2} \w+ \d{4}')),
      DAY
    ) = 1 THEN 'NDD'
    ELSE 'Non NDD'
  END AS ndd_flag,

  -- ── 6. Same Day Pickup flag ─────────────────────────────────────────
  -- NULL if picked_up_date is NULL (order not yet picked up)
  CASE
    WHEN o.picked_up_date IS NULL OR o.picked_up_date = '' THEN NULL
    WHEN DATE_DIFF(
      DATE(SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', o.picked_up_date)),
      PARSE_DATE('%d %b %Y',
        REGEXP_EXTRACT(o.channel_created_at, r'^\d{1,2} \w+ \d{4}')),
      DAY
    ) = 0 THEN 'Same Day Pickup'
    ELSE 'Non Same Day Pickup'
  END AS same_day_pickup_flag

  -- ── ADD NEW FIELDS BELOW THIS LINE ─────────────────────────────────

FROM unique_orders o

LEFT JOIN zone_deduped zm
  ON  CAST(zm.pickup_pincode   AS STRING) = o.pickup_pincode
  AND CAST(zm.customer_pincode AS STRING) = o.customer_pincode

LEFT JOIN `shiprocket-496417.shiprocket.ndr_data` ndr
  ON ndr.awb = o.awb
