
{{
    config(
        materialized='incremental',
        unique_key='surr_key'

    )
}}

with POS_TX_DSCNT as (
     select
		DSCNT_LINE_NBR NUMBER(38,0) AS (value:c1::NUMBER(38,0)),
		SALE_LINE_NBR NUMBER(38,0) AS (value:c2::NUMBER(38,0)),
		TX_NBR NUMBER(38,0) AS (value:c3::NUMBER(38,0)),
		POS_LOC_ID NUMBER(38,0) AS (value:c4::NUMBER(38,0)),
		TERM_NBR NUMBER(38,0) AS (value:c5::NUMBER(38,0)),
		DIV_NBR NUMBER(38,0) AS (value:c6::NUMBER(38,0)),
		BIZ_DT NUMBER(38,0) AS (value:c7::NUMBER(38,0)),
		ADJ_FLG VARCHAR(1) AS (value:c8::varchar(1)),
		STORE_ID NUMBER(38,0) AS (value:c9::NUMBER(38,0)),
		DSCNT_ID_NBR VARCHAR(10) AS (value:c10::varchar(10)),
		DIV_DSCNT_CD VARCHAR(2) AS (value:c11::varchar(2)),
		DIV_DSCNT_TYPE VARCHAR(1) AS (value:c12::varchar(1)),
		MERCH_DSCNT_CD NUMBER(2,0) AS (value:c13::NUMBER(2,0)),
		LOCAL_DSCNT_AMT NUMBER(18,4) AS (value:c14::NUMBER(18,4)),
		HOST_DSCNT_AMT NUMBER(18,4) AS (value:c15::NUMBER(18,4)),
		DSCNT_PERCENT NUMBER(3,0) AS (value:c16::NUMBER(3,0)),
		RTL_DSCNT_EMBED_TAX NUMBER(18,4) AS (value:c17::NUMBER(2,0)),
		COUP_CD NUMBER(2,0) AS (value:c18::NUMBER(2,0)),
		COUP_NBR VARCHAR(10) AS (value:c19::varchar(10)),
		DSCNT_REAS_CD NUMBER(2,0) AS (value:c20::NUMBER(2,0)),
		RCPT_SEQ_NBR NUMBER(38,0) AS (value:c21::NUMBER(38,0)),
        division,
        run_dt,
        SHA2_HEX(concat_ws('~',dscnt_line_nbr,sale_line_nbr,tx_nbr,term_nbr,div_nbr,pos_loc_id)) as surr_key,
        to_timestamp('{{ var("run_ts") }}') as run_ts  from {{source('dfs_stage','ext_pos_tx_dscnt') }}
)

select * from POS_TX_DSCNT