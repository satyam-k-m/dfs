
{{
    config(
        materialized='incremental',
        unique_key='surr_key'

    )
}}

with POS_TNDR as (
     select
		PAY_LINE_NBR NUMBER(38,0)  AS (value:c1:: NUMBER(38,0)),
		TX_NBR NUMBER(38,0)  AS (value:c1:: NUMBER(38,0)),
		DIV_NBR NUMBER(38,0)  AS (value:c1:: NUMBER(38,0)),
		TERM_NBR NUMBER(38,0)  AS (value:c1:: NUMBER(38,0)),
		POS_LOC_ID NUMBER(38,0)  AS (value:c1:: NUMBER(38,0)),
		BIZ_DT NUMBER(38,0)  AS (value:c1:: NUMBER(38,0)),
		ADJ_FLG VARCHAR(1)  AS (value:c1:: VARCHAR(1)),
		CURR_CD VARCHAR(3)  AS (value:c1:: VARCHAR(3)),
		PAY_MTHD NUMBER(38,0)  AS (value:c1:: NUMBER(38,0)),
		FOR_CURR NUMBER(18,4)  AS (value:c1:: NUMBER(18,4)),
		TEN_AMT NUMBER(18,4)  AS (value:c1:: NUMBER(18,4)),
		EXCH_RATE NUMBER(18,4)  AS (value:c1:: NUMBER(18,4)),
		RCPT_SEQ_NBR NUMBER(38,0)  AS (value:c1:: NUMBER(38,0)),
		ADDITION_DATA VARCHAR(20)  AS (value:c1:: VARCHAR(20)),
		CHNG_RND NUMBER(18,4)  AS (value:c1:: NUMBER(18,4)),
        division,
        run_dt,
        SHA2_HEX(concat_ws('~',pay_line_nbr,tx_nbr,term_nbr,div_nbr,pos_loc_id)) as surr_key,
        to_timestamp('{{ var("run_ts") }}') as run_ts  from {{source('dfs_stage','ext_POS_TNDR') }}
)

select * from POS_TNDR