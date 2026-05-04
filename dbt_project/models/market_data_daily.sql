{{
  config(
    materialized='table',
    partition_by={
      "field": "date_id",
      "data_type": "int64",
      "range": {
        "start": 0,
        "end": 2000,
        "interval": 1
      }
    },
    cluster_by=['symbol_id']
  )
}}

-- This model assumes an external table or raw table exists named `raw_market_data`
-- that has been loaded from GCS or directly written by Flink.
-- We aggregate it to daily level to power Looker Studio efficiently.

WITH source_data AS (
    SELECT * FROM {{ source('jane_street', 'raw_market_data') }}
)

SELECT
    date_id,
    symbol_id,
    COUNT(time_id) as tick_count,
    AVG(weight) as avg_weight,
    AVG(feature_00) as avg_feature_00,
    AVG(feature_01) as avg_feature_01,
    AVG(responder_6) as avg_responder_6
FROM source_data
GROUP BY 
    date_id, 
    symbol_id
