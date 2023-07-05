

with RFND_ORGNL_TX_RFRNC as (
     select
		value:c1::NUMBER(38,0) AS ORIG_TX_NBR,
        value:c2::NUMBER(38,0) AS ORIG_TERM_NBR,
        value:c3::NUMBER(38,0) AS ORIG_DIV_NBR,
        value:c4::NUMBER(38,0) AS ORIG_POS_LOC_ID,
        value:c5::NUMBER(38,0) AS ORIG_BIZ_DT,
        value:c6::varchar(1) AS ORIG_ADJ_FLG,
        value:c7::NUMBER(38,0) AS ORIG_TX_DT,
        value:c8::NUMBER(38,0) AS RFND_TX_NBR,
        value:c9::NUMBER(38,0) AS RFND_TERM_NBR,
        value:c10::NUMBER(38,0) AS RFND_DIV_NBR,
        value:c11::NUMBER(38,0) AS RFND_POS_LOC_ID ,
        value:c12::NUMBER(38,0) AS RFND_BIZ_DT,
        value:c13::varchar(1) AS RFND_ADJ_FLG,
        value:c14::NUMBER(38,0) AS RFND_TX_DT,
        division,
        run_dt,
        SHA2_HEX(concat_ws('~',ORIG_TX_NBR,ORIG_TERM_NBR)) as surr_key,
        to_timestamp('2023-06-22') as run_ts  from insight_dev.ins_bkp.ext_rfnd_tx_rf
)

select * from RFND_ORGNL_TX_RFRNC