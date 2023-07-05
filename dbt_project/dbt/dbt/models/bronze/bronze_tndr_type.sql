
{{
    config(
        materialized='incremental',
        unique_key='surr_key'

    )
}}

with TNDR_TYPE as (
     select
		TEN_TYPE NUMBER(38,0) AS (value:c1::NUMBER(38,0)),
		DIV_NBR NUMBER(38,0) AS (value:c2::NUMBER(38,0)),
		SHORT_DESC VARCHAR(4) AS (value:c3::varchar(4)),
		LONG_DESC VARCHAR(30) AS (value:c4::varchar(30)),
		CURR_CARD_TYPE VARCHAR(1) AS (value:c5::varchar(1)),
		SRC_UPDT_DT NUMBER(38,0) AS (value:c6::NUMBER(38,0)),
        division,
        run_dt,
        SHA2_HEX(concat_ws('~',ten_type,div_nbr)) as surr_key,
        to_timestamp('{{ var("run_ts") }}') as run_ts  from {{source('dfs_stage','ext_tndr_type') }}
)

select * from TNDR_TYPE