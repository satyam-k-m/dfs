
{{
    config(
        MATERIALIZED='INCREMENTAL',
        UNIQUE_KEY='SURR_KEY'

    )
}}

WITH LOCAL_CURRENCY AS (
    SELECT 
        CURR_CD,
        DIV_NBR,
        CURR_DESC,
        CURR_SHORT_DESC,
        LOCAL_CURR_IND,
        CURR_UNIT,
        DIVISION,
        RUN_DT,
        SURR_KEY,
        RUN_TS
    FROM {{ ref("bronze_local_currency")}}

    {% if is_incremental() %}
            where run_dt = to_date('{{ var("run_ts") }}')
    {% endif %}
)
SELECT *
FROM LOCAL_CURRENCY
