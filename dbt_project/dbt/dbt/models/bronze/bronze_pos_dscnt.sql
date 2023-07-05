
{{
    config(
        materialized='incremental',
        unique_key='surr_key'

    )
}}

with POS_DSCNT as (
     select
		DIV_NBR NUMBER(38,0) AS (value:c1:: NUMBER(38,0)),
		DIV_DSCNT_CD VARCHAR(2) AS (value:c2:: VARCHAR(2)),
		DIV_DSCNT_DESC VARCHAR(30) AS (value:c3:: VARCHAR(30)),
		MERCH_DSCNT_CD NUMBER(2,0) AS (value:c4:: NUMBER(2,0)),
		MD_DSCNT_CD VARCHAR(1) AS (value:c5:: VARCHAR(1)),
        division,
        run_dt,
        SHA2_HEX(concat_ws('~',div_nbr,div_dscnt_cd)) as surr_key,
        to_timestamp('{{ var("run_ts") }}') as run_ts  from {{source('dfs_stage','ext_POS_DSCNT') }}
)

select * from POS_DSCNT