
{{
    config(
        materialized='incremental',
        unique_key='surr_key'

    )
}}

with TX_TYPE as (
     select
		DIV_NBR NUMBER(38,0) AS (value:c1::NUMBER(38,0)),
		APP_CD VARCHAR(2) AS (value:c2::varchar(2)),
		TX_TYPE VARCHAR(2) AS (value:c3::varchar(2)),
		SHORT_DESC VARCHAR(30) AS (value:c4::varchar(30)),
        division,
        run_dt,
        SHA2_HEX(concat_ws('~',div_nbr,app_cd,tx_type)) as surr_key,
        to_timestamp('{{ var("run_ts") }}') as run_ts  from {{source('dfs_stage','ext_tx_type') }}
)

select * from TX_TYPE