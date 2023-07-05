
{{
    config(
        materialized='incremental',
        unique_key='surr_key'

    )
}}

with POS_TX_LINE as (
     select
		SALE_LINE_NBR NUMBER(38,0) AS (value:c1::NUMBER(38,0)),
		PROCESS_DT NUMBER(38,0) AS (value:c2::NUMBER(38,0)),
		DIVISON_NUMBER NUMBER(38,0) AS (value:c3::NUMBER(38,0)),
		TX_NBR NUMBER(38,0) AS (value:c4::NUMBER(38,0)),
		DIV_NBR NUMBER(38,0) AS (value:c5::NUMBER(38,0)),
		POS_LOC_ID NUMBER(38,0) AS (value:c6::NUMBER(38,0)),
		TERM_NBR NUMBER(38,0) AS (value:c7::NUMBER(38,0)),
		BIZ_DT NUMBER(38,0) AS (value:c8::NUMBER(38,0)),
		ADJ_FLG VARCHAR(1) AS (value:c9::varchar(1)),
		STORE_ID NUMBER(38,0) AS (value:c10::NUMBER(38,0)),
		PROMO_CD NUMBER(4,0) AS (value:c11::NUMBER(38,0)),
		DSKU_ID NUMBER(9,0) AS (value:c12::NUMBER(9,0)),
		CSKU_ID NUMBER(38,0) AS (value:c13::NUMBER(38,0)),
		LINE_GRP_NBR NUMBER(38,0) AS (value:c14::NUMBER(38,0)),
		PREORD_FLG VARCHAR(1) AS (value:c15::varchar(1)),
		INV_RLS_LOC NUMBER(38,0) AS (value:c16::NUMBER(38,0)),
		UNIT_PRICE NUMBER(18,4) AS (value:c17::NUMBER(18,4)),
		ITEM_QTY NUMBER(5,0) AS (value:c18::NUMBER(5,0)),
		EXT_RTL NUMBER(18,4) AS (value:c19::NUMBER(18,4)),
		PRICE_OVERRIDE VARCHAR(1) AS (value:c20::varchar(1)),
		ORIG_PRICE NUMBER(18,4) AS (value:c21::NUMBER(18,4)),
		POST_TO_MCS VARCHAR(1) AS (value:c22::varchar(1)),
		DUTY_TYPE VARCHAR(1) AS (value:c23::varchar(1)),
		TAX_FLG_1 VARCHAR(1) AS (value:c24::varchar(1)),
		TAX_FLG_2 VARCHAR(1) AS (value:c25::varchar(1)),
		TAX_FLG_3 VARCHAR(1) AS (value:c26::varchar(1)),
		TAX_FLG_4 VARCHAR(1) AS (value:c27::varchar(1)),
		LOCAL_TAX_1 NUMBER(18,4) AS (value:c28::NUMBER(18,4)),
		LOCAL_TAX_2 NUMBER(18,4) AS (value:c29::NUMBER(18,4)),
		LOCAL_TAX_3 NUMBER(18,4) AS (value:c30::NUMBER(18,4)),
		LOCAL_TAX_4 NUMBER(18,4) AS (value:c31::NUMBER(18,4)),
		RTL_EMBED_TAX NUMBER(18,4) AS (value:c32::NUMBER(18,4)),
		RTL_EMBED_TAX_RATE NUMBER(18,4) AS (value:c33::NUMBER(18,4)),
		LOCAL_NET_AMT NUMBER(18,4) AS (value:c34::NUMBER(18,4)),
		HOST_NET_AMT NUMBER(18,4) AS (value:c35::NUMBER(18,4)),
		HOST_TAX_1 NUMBER(18,4) AS (value:c36::NUMBER(18,4)),
		HOST_TAX_2 NUMBER(18,4) AS (value:c37::NUMBER(18,4)),
		HOST_TAX_3 NUMBER(18,4) AS (value:c38::NUMBER(18,4)),
		HOST_TAX_4 NUMBER(18,4) AS (value:c39::NUMBER(18,4)),
		SA_ID VARCHAR(30) AS (value:c40::varchar(30)),
		COUP_CD NUMBER(2,0) AS (value:c41::NUMBER(2,0)),
		COUP_NBR VARCHAR(10) AS (value:c42::varchar(10)),
		ORIG_SA_ID NUMBER(11,0) AS (value:c43::NUMBER(11,0)),
		RCPT_SEQ_NBR NUMBER(38,0) AS (value:c44::NUMBER(38,0)),
		ENT_DATA VARCHAR(15) AS (value:c45::varchar(15)),
		DIV_DATA_1 VARCHAR(9) AS (value:c46::varchar(9)),
		DIV_DATA_2 VARCHAR(20) AS (value:c47::varchar(20)),
		PLU_LOC_NBR NUMBER(4,0) AS (value:c48::NUMBER(4,0)),
		NOT_AUTH_FLG VARCHAR(1) AS (value:c49::varchar(1)),
		ITEM_PICK_TYPE VARCHAR(1) AS (value:c50::varchar(1)),
		HOST_CURR_UNIT_PRICE NUMBER(18,4) AS (value:c51::NUMBER(18,4)),
		HOST_CURR_EXT_PRICE NUMBER(18,4) AS (value:c52::NUMBER(18,4)),
		DUTY_PAY_AMT NUMBER(18,4) AS (value:c53::NUMBER(18,4)),
		IGATE_REFERENCE VARCHAR(40) AS (value:c54::varchar(40)),
        division,
        run_dt,
        SHA2_HEX(concat_ws('~',sale_line_nbr,tx_nbr,term_nbr,divison_number,pos_loc_id,div_nbr)) as surr_key,
        to_timestamp('{{ var("run_ts") }}') as run_ts  from {{source('dfs_stage','ext_pos_tx_line') }}
)

select * from POS_TX_LINE