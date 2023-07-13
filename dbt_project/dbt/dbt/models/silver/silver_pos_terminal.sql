
{{
    config(
        materialized='incremental',
        unique_key='surr_key'

    )
}}

with POS_TRMNL as (
     select
		POS_LOCATION_ID,
		DIVISON_NUMBER,
		TERMINAL_NUMBER,
        division,
        run_dt,
        surr_key,
        run_ts  from {{ref('bronze_pos_trmnl') }}

        {% if is_incremental() %}
            where run_dt = to_date('{{ var("run_ts") }}')
        {% endif %}
)

select * from POS_TRMNL
