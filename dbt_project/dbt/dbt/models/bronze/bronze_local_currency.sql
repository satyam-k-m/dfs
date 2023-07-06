
{{
    config(
        MATERIALIZED='INCREMENTAL',
        UNIQUE_KEY='SURR_KEY'

    )
}}

WITH LOCAL_CURRENCY AS (
    SELECT 
        VALUE:c1::VARCHAR(3) AS CURR_CD,
        VALUE:c2::NUMBER(38, 0) AS DIV_NBR,
        VALUE:c3::VARCHAR(500) AS CURR_DESC,
        VALUE:c4::VARCHAR(6) AS CURR_SHORT_DESC,
        VALUE:c5::VARCHAR(100) AS LOCAL_CURR_IND,
        VALUE:c6::NUMBER(38, 0) AS CURR_UNIT,
        DIVISION,
        RUN_DT,
        SHA2_HEX(CONCAT_WS('~',CURR_CD,DIV_NBR)) AS SURR_KEY,
        TO_TIMESTAMP('{{ var("run_ts") }}') AS RUN_TS
    FROM {{ source('dfs_stage', 'ext_lcl_crrncy') }}

    {% if is_incremental() %}
            where run_dt = to_date('{{ var("run_ts") }}')
    {% endif %}
)
SELECT *
FROM LOCAL_CURRENCY
