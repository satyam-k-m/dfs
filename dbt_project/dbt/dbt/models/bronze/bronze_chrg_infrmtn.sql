
{{
    config(
        materialized='incremental',
        unique_key='surr_key'

    )
}}

with CHRG_INFRMTN as (
     select
		PAY_LINE_NBR NUMBER(38,0) AS (value:c1:: NUMBER(38,0)),
		TX_NBR NUMBER(38,0) AS (value:c2:: NUMBER(38,0)),
		TERM_NBR NUMBER(38,0) AS (value:c3:: NUMBER(38,0)),
		DIV_NBR NUMBER(38,0) AS (value:c4:: NUMBER(38,0)),
		POS_LOC_ID NUMBER(38,0) AS (value:c5:: NUMBER(38,0)),
		BIZ_DT NUMBER(38,0) AS (value:c6:: NUMBER(38,0)),
		ADJ_FLG VARCHAR(1) AS (value:c7:: VARCHAR(1)),
		ACC_NBR NUMBER(19,0) AS (value:c8:: NUMBER(19,0)),
		ACC_TYPE VARCHAR(10) AS (value:c9:: VARCHAR(10)),
		EXP_DT NUMBER(4,0) AS (value:c10:: NUMBER(4,0)),
		APP_CD VARCHAR(10) AS (value:c11:: VARCHAR(10)),
        division,
        run_dt,
        SHA2_HEX(concat_ws('~',pay_line_nbr,tx_nbr,term_nbr,div_nbr,pos_loc_id)) as surr_key,
        to_timestamp('{{ var("run_ts") }}') as run_ts  from {{source('dfs_stage','EXT_CHRG_INFRMTN') }}
)

select * from CHRG_INFRMTN