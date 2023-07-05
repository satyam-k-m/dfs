

{{
    config(
        materialized='incremental',
        unique_key='surr_key'

    )
}}

with pos_tx as (
    select
		TX_NBR NUMBER(38,0) AS (value:c1:: NUMBER(38,0)),
		POS_LOC_ID NUMBER(38,0) AS (value:c2:: NUMBER(38,0)),
		DIV_NBR NUMBER(38,0) AS (value:c3:: NUMBER(38,0)),
		TERM_NBR NUMBER(38,0) AS (value:c4:: NUMBER(38,0)),
		BIZ_DT NUMBER(38,0) AS (value:c5:: NUMBER(38,0)),
		ADJ_FLG VARCHAR(1) AS (value:c6:: VARCHAR(1)),
		TX_DT NUMBER(38,0) AS (value:c7:: NUMBER(38,0)),
		TX_TIME NUMBER(6,0) AS (value:c8:: NUMBER(6,0)),
		CURR_CD VARCHAR(3) AS (value:c9:: VARCHAR(3)),
		POS_TX_TYPE VARCHAR(2) AS (value:c10:: VARCHAR(2)),
		MERCH_TX_TYPE VARCHAR(2) AS (value:c11:: VARCHAR(2)),
		CUST_ID NUMBER(38,0)  AS (value:c12:: NUMBER(38,0)),
		LOCAL_NAT_CD NUMBER(38,0) AS (value:c13:: NUMBER(38,0)),
		REKEY_IND VARCHAR(1) AS (value:c14:: VARCHAR(1)),
		ITEM_CNT NUMBER(38,0) AS (value:c15:: NUMBER(38,0)),
		CASHIER_NBR NUMBER(11,0) AS (value:c16:: NUMBER(11,0)),
		LOCAL_NET_AMT NUMBER(18,4) AS (value:c17:: NUMBER(18,4)),
		LOCAL_SUBTOT NUMBER(18,4) AS (value:c18:: NUMBER(18,4)),
		LOCAL_TOT_TAX NUMBER(18,4) AS (value:c19:: NUMBER(18,4)),
		TOT_PAY_AMT NUMBER(18,4) AS (value:c20:: NUMBER(18,4)),
		HOST_CUR_CD VARCHAR(3) AS (value:c21:: VARCHAR(3)),
		HOST_NET_AMT NUMBER(18,4) AS (value:c22:: NUMBER(18,4)),
		HOST_SUBTOT NUMBER(18,4) AS (value:c23:: NUMBER(18,4)),
		HOST_TOT_TAX NUMBER(18,4) AS (value:c24:: NUMBER(18,4)),
		RFND_REAS NUMBER(2,0) AS (value:c25:: NUMBER(2, 0)),
		DUTY_PAY_FLG VARCHAR(1) AS (value:c26:: VARCHAR(1)),
		MEMBERSHIP_CARD_NUMBER VARCHAR(100) AS (value:c27:: VARCHAR(100)),
		CUST_FNAME VARCHAR(50) AS (value:c28:: VARCHAR(50)),
		CUST_LNAME VARCHAR(50) AS (value:c29:: VARCHAR(50)),
		CUST_GNDR VARCHAR(1) AS (value:c30:: VARCHAR(1)),
		CUST_PSPRT VARCHAR(12) AS (value:c31:: VARCHAR(12)),
		CUST_FLT VARCHAR(20) AS (value:c32:: VARCHAR(20)),
		CUST_DEPDATE NUMBER(8,0) AS (value:c33:: NUMBER(8,0)),
		FLT_DST VARCHAR(20) AS (value:c34:: VARCHAR(20)),
		FLT_CLSS VARCHAR(1) AS (value:c35:: VARCHAR(1)),
		FLT_DPNT VARCHAR(3) AS (value:c36:: VARCHAR(3)),
		FLT_SEAT VARCHAR(3) AS (value:c37:: VARCHAR(3)),
		FLT_CFLT VARCHAR(20) AS (value:c38:: VARCHAR(20)),
		FLT_CDEPDATE NUMBER(8,0) AS (value:c39:: NUMBER(8,0)),
		FLT_CDST VARCHAR(20) AS (value:c40:: VARCHAR(20)),
		ID_TYPE NUMBER(2,0) AS (value:c41:: NUMBER(2,0)),
		ID_NO VARCHAR(20)  AS (value:c42:: VARCHAR(20)),
		division,
		run_dt,
		SHA2_HEX(concat_ws('~',TX_NBR,POS_LOC_ID,DIV_NBR,TERM_NBR)) as surr_key,
		to_timestamp('{{ var("run_ts") }}') as run_ts  from {{source('dfs_stage','ext_pos_tx') }}
)
		
select * from pos_tx
