

with POS_TX_DSCNT as (
     select
		value:c1::NUMBER(38,0) AS DSCNT_LINE_NBR,
		value:c2::NUMBER(38,0) AS SALE_LINE_NBR,
		value:c3::NUMBER(38,0) AS TX_NBR,
		value:c4::NUMBER(38,0) AS POS_LOC_ID,
		value:c5::NUMBER(38,0) AS TERM_NBR,
		value:c6::NUMBER(38,0) AS DIV_NBR,
		value:c7::NUMBER(38,0) AS BIZ_DT,
		value:c8::varchar(1) AS ADJ_FLG,
		value:c9::NUMBER(38,0) AS STORE_ID,
		value:c10::varchar(10) AS DSCNT_ID_NBR,
		value:c11::varchar(2) AS DIV_DSCNT_CD,
		value:c12::varchar(1) AS DIV_DSCNT_TYPE,
		value:c13::NUMBER(2,0) AS MERCH_DSCNT_CD,
		value:c14::NUMBER(18,4) AS LOCAL_DSCNT_AMT,
		value:c15::NUMBER(18,4) AS HOST_DSCNT_AMT ,
		value:c16::NUMBER(3,0) AS DSCNT_PERCENT,
		value:c17::NUMBER(2,0) AS RTL_DSCNT_EMBED_TAX,
		value:c18::NUMBER(2,0) AS COUP_CD,
		value:c19::varchar(10) AS COUP_NBR ,
		value:c20::NUMBER(2,0) AS DSCNT_REAS_CD,
		value:c21::NUMBER(38,0) AS RCPT_SEQ_NBR,
        division,
        run_dt,
        SHA2_HEX(concat_ws('~',dscnt_line_nbr,sale_line_nbr,tx_nbr,term_nbr,div_nbr,pos_loc_id)) as surr_key,
        to_timestamp('2023-06-22') as run_ts  from insight_dev.ins_bkp.ext_pos_tx_dct
)

select * from POS_TX_DSCNT