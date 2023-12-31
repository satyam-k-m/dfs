
{{
    config(
        materialized='incremental',
        unique_key='surr_key'

    )
}}

with POS_TX_LINE as (
     select
		value:c1::NUMBER(38,0) AS SALE_LINE_NBR,
        value:c2::NUMBER(38,0) AS PROCESS_DT ,
        value:c3::NUMBER(38,0) AS DIVISON_NUMBER ,
        value:c4::NUMBER(38,0) AS TX_NBR,
        value:c5::NUMBER(38,0) AS DIV_NBR ,
        value:c6::NUMBER(38,0) AS POS_LOC_ID,
        value:c7::NUMBER(38,0) AS TERM_NBR ,
        value:c8::NUMBER(38,0) AS BIZ_DT,
        value:c9::varchar(1) AS ADJ_FLG,
        value:c10::NUMBER(38,0) AS STORE_ID,
        value:c11::NUMBER(38,0) AS PROMO_CD,
        value:c12::NUMBER(9,0) AS DSKU_ID,
        value:c13::NUMBER(38,0) AS CSKU_ID,
        value:c14::NUMBER(38,0) AS LINE_GRP_NBR,
        value:c15::varchar(1) AS PREORD_FLG,
        value:c16::NUMBER(38,0) AS INV_RLS_LOC,
        value:c17::NUMBER(18,4) AS UNIT_PRICE,
        value:c18::NUMBER(5,0) AS ITEM_QTY ,
        value:c19::NUMBER(18,4) AS EXT_RTL ,
        value:c20::varchar(1) AS PRICE_OVERRIDE,
        value:c21::NUMBER(18,4) AS ORIG_PRICE ,
        value:c22::varchar(1) AS POST_TO_MCS ,
        value:c23::varchar(1) AS DUTY_TYPE ,
        value:c24::varchar(1) AS TAX_FLG_1 ,
        value:c25::varchar(1) AS TAX_FLG_2,
        value:c26::varchar(1) AS TAX_FLG_3,
        value:c27::varchar(1) AS TAX_FLG_4,
        value:c28::NUMBER(18,4) AS LOCAL_TAX_1,
        value:c29::NUMBER(18,4) AS LOCAL_TAX_2,
        value:c30::NUMBER(18,4) AS LOCAL_TAX_3,
        value:c31::NUMBER(18,4) AS LOCAL_TAX_4,
        value:c32::NUMBER(18,4) AS RTL_EMBED_TAX,
        value:c33::NUMBER(18,4) AS RTL_EMBED_TAX_RATE,
        value:c34::NUMBER(18,4) AS LOCAL_NET_AMT,
        value:c35::NUMBER(18,4) AS HOST_NET_AMT,
        value:c36::NUMBER(18,4) AS HOST_TAX_1 ,
        value:c37::NUMBER(18,4) AS HOST_TAX_2,
        value:c38::NUMBER(18,4) AS HOST_TAX_3 ,
        value:c39::NUMBER(18,4) AS HOST_TAX_4,
        value:c40::varchar(30) AS SA_ID,
        value:c41::NUMBER(2,0) AS COUP_CD,
        value:c42::varchar(10) AS COUP_NBR,
        value:c43::NUMBER(11,0) AS ORIG_SA_ID,
        value:c44::NUMBER(38,0) RCPT_SEQ_NBR,
        value:c45::varchar(15) AS ENT_DATA,
        value:c46::varchar(9) AS DIV_DATA_1,
        value:c47::varchar(20) AS DIV_DATA_2,
        value:c48::NUMBER(4,0) AS PLU_LOC_NBR,
        value:c49::varchar(1) AS NOT_AUTH_FLG,
        value:c50::varchar(1) AS ITEM_PICK_TYPE,
        value:c51::NUMBER(18,4) AS HOST_CURR_UNIT_PRICE,
        value:c52::NUMBER(18,4) AS HOST_CURR_EXT_PRICE ,
        value:c53::NUMBER(18,4) AS DUTY_PAY_AMT ,
        Value:c54::varchar(40) AS IGATE_REFERENCE,
        division,
        run_dt,
        SHA2_HEX(concat_ws('~',sale_line_nbr,tx_nbr,term_nbr,divison_number,pos_loc_id,div_nbr)) as surr_key,
        to_timestamp('{{ var("run_ts") }}') as run_ts  from {{source('dfs_stage','ext_pos_tx_ln') }}

        {% if is_incremental() %}
            where run_dt = to_date('{{ var("run_ts") }}')
        {% endif %}
)

select * from POS_TX_LINE
