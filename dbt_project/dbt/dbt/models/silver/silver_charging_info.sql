
{{
    config(
        materialized='incremental',
        unique_key='surr_key'

    )
}}


with CHRG_INFRMTN as (
     select
		PAY_LINE_NBR,
        TX_NBR,
        TERM_NBR,
        DIV_NBR,
        POS_LOC_ID,
        BIZ_DT,
        ADJ_FLG,
        ACC_NBR,
        ACC_TYPE,
        EXP_DT,
        APP_CD,
        division,
        run_dt,
        surr_key,
        run_ts  from {{ref('bronze_chrg_infrmtn') }}

        {% if is_incremental() %}
            where run_dt = to_date('{{ var("run_ts") }}')
        {% endif %}
)

select * from CHRG_INFRMTN
