
{{
    config(
        materialized='incremental',
        unique_key='surr_key'

    )
}}

with division as (
     select
        DIV_NBR,
        DIV_NAME, 
        CITY,
        STATE,
        ZIP_CD,
        DVSN_RPT_HDNG,
        TRAD_COMP_CD,
        DB_COST_FLG,
        CREATE_PREORD_TRANS_AUD_REC,
        POPI_CNT_FLG,
        RCENT_CD,
        AUTO_WRITE_OFF_LIMIT,
        MRPU_ENTRY,
        SCLS_REALIGN_DUR_PI,
        division,
        run_dt,
        surr_key,
        run_ts  from {{ref('bronze_divison') }}

        {% if is_incremental() %}
            where run_dt = to_date('{{ var("run_ts") }}')
        {% endif %}
)

select * from division
