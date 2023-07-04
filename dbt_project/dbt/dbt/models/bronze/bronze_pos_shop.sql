
{{
    config(
        MATERIALIZED='INCREMENTAL',
        UNIQUE_KEY='SURR_KEY'

    )
}}

WITH POS_SHOP AS (
    SELECT 
        VALUE:c1::NUMBER(38, 0) AS POS_LOCATION_ID,
        VALUE:c2::NUMBER(38, 0) AS DIVISON_NUMBER,
        DIVISION,
        RUN_DT,
        SHA2_HEX(CONCAT_WS('~',POS_LOCATION_ID,DIVISON_NUMBER)) AS SURR_KEY,
        TO_TIMESTAMP('{{ var("run_ts") }}') AS RUN_TS
    FROM {{ source('dfs_stage', 'ext_pos_shop') }}
)
SELECT *
FROM POS_SHOP
