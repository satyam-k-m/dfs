
{{
    config(
        materialized='incremental',
        unique_key='surr_key'

    )
}}

with POS_TRMNL as (
     select
		value:c1::NUMBER(38,0) AS POS_LOCATION_ID,
		value:c2::NUMBER(38,0) AS DIVISON_NUMBER,
		value:c3::NUMBER(38,0) AS TERMINAL_NUMBER,
        division,
        run_dt,
        SHA2_HEX(concat_ws('~',pos_location_id,divison_number,terminal_number)) as surr_key,
        to_timestamp('{{ var("run_ts") }}') as run_ts  from {{source('dfs_stage','ext_pos_trmnl') }}

        {% if is_incremental() %}
            where run_dt = to_date('{{ var("run_ts") }}')
        {% endif %}
)

select * from POS_TRMNL
