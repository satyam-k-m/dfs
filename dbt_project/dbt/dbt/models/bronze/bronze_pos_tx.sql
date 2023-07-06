

{{
    config(
        materialized='incremental',
        unique_key='surr_key'

    )
}}

with pos_tx as (
    select
		value:c1:: NUMBER(38,0) AS TX_NBR,
		value:c2:: NUMBER(38,0) AS POS_LOC_ID,
		value:c3:: NUMBER(38,0) AS DIV_NBR,
		value:c4:: NUMBER(38,0) AS TERM_NBR,
		value:c5:: NUMBER(38,0) AS BIZ_DT ,
		value:c6:: VARCHAR(1) AS ADJ_FLG,
		value:c7:: NUMBER(38,0) AS	TX_DT,
		value:c8:: NUMBER(6,0) AS TX_TIME,
		value:c9:: VARCHAR(3) AS CURR_CD ,
		value:c10:: VARCHAR(2) AS POS_TX_TYPE,
		value:c11:: VARCHAR(2) AS MERCH_TX_TYPE,
		value:c12:: NUMBER(38,0) AS CUST_ID,
		value:c13:: NUMBER(38,0) AS LOCAL_NAT_CD,
		value:c14:: VARCHAR(1) AS REKEY_IND,
		value:c15:: NUMBER(38,0) AS ITEM_CNT,
		value:c16:: NUMBER(11,0) AS CASHIER_NBR ,
		value:c17:: NUMBER(18,4) AS LOCAL_NET_AMT,
		value:c18:: NUMBER(18,4) AS LOCAL_SUBTOT,
		value:c19:: NUMBER(18,4) AS LOCAL_TOT_TAX ,
		value:c20:: NUMBER(18,4) AS TOT_PAY_AMT,
		value:c21:: VARCHAR(3) AS HOST_CUR_CD,
		value:c22:: NUMBER(18,4) AS HOST_NET_AMT,
        value:c23:: NUMBER(18,4) AS HOST_SUBTOT ,
        value:c24:: NUMBER(18,4) AS HOST_TOT_TAX,
        value:c25:: NUMBER(2, 0) AS RFND_REAS ,
        value:c26:: VARCHAR(1) AS DUTY_PAY_FLG,
        value:c27:: VARCHAR(100) AS MEMBERSHIP_CARD_NUMBER ,
        value:c28:: VARCHAR(50) AS CUST_FNAME ,
        value:c29:: VARCHAR(50) AS CUST_LNAME,
        value:c30:: VARCHAR(1) AS CUST_GNDR,
        value:c31:: VARCHAR(12) AS CUST_PSPRT,
        value:c32:: VARCHAR(20) AS CUST_FLT,
        value:c33:: NUMBER(8,0) AS CUST_DEPDATE,
        value:c34:: VARCHAR(20) AS FLT_DST ,
        value:c35:: VARCHAR(1) AS FLT_CLSS,
        value:c36:: VARCHAR(3) AS FLT_DPNT ,
        value:c37:: VARCHAR(3) AS FLT_SEAT,
        value:c38:: VARCHAR(20) AS FLT_CFLT ,
        value:c39:: NUMBER(8,0) AS FLT_CDEPDATE,
        value:c40:: VARCHAR(20) AS  FLT_CDST ,
        value:c41:: NUMBER(2,0) AS ID_TYPE,
        value:c42:: VARCHAR(20) AS ID_NO,
		division,
		run_dt,
		SHA2_HEX(concat_ws('~',TX_NBR,POS_LOC_ID,DIV_NBR,TERM_NBR)) as surr_key,
		to_timestamp('{{ var("run_ts") }}') as run_ts  from {{source('dfs_stage','ext_pos_tx') }}

		{% if is_incremental() %}
            where run_dt = to_date('{{ var("run_ts") }}')
        {% endif %}
)
		
select * from pos_tx
