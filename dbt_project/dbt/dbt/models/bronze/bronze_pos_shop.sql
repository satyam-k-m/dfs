
{{
    config(
        materialized='incremental',
        unique_key='surr_key'

    )
}}

with POS_SHOP as (
     select
		POS_LOCATION_ID NUMBER(38,0) AS (value:c1:: NUMBER(38,0)),
		DIVISON_NUMBER NUMBER(38,0) AS (value:c2:: NUMBER(38,0)),
        division,
        run_dt,
        SSHA2_HEX(concat_ws('~',pos_location_id,divison_number)) as surr_key,
        to_timestamp('{{ var("run_ts") }}') as run_ts  from {{source('dfs_stage','ext_pos_shop') }}
)

select * from POS_SHOP