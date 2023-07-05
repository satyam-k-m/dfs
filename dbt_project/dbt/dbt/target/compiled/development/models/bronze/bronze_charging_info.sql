

with charging_info as (
     select
        value:c1:: NUMBER(38,0) as PAY_LINE_NBR,
        value:c2:: NUMBER(38,0) as TX_NBR,
        value:c3:: NUMBER(38,0) as TERM_NBR,
        value:c4:: NUMBER(38,0) as DIV_NBR,
        value:c5:: NUMBER(38,0) as POS_LOC_ID,
        value:c6:: NUMBER(38,0) as BIZ_DT,
        value:c7:: VARCHAR(1) as ADJ_FLG,
        value:c8::  NUMBER(19,0) as ACC_NBR,
        value:c9:: VARCHAR(10) as ACC_TYPE,
        value:c10:: NUMBER(38,0) as EXP_DT,
        value:c11:: NUMBER(10) as APP_CD,
        division,
        run_dt,
        SHA2_HEX(concat_ws('~',PAY_LINE_NBR,TX_NBR,TERM_NBR,DIV_NBR,POS_LOC_ID)) as surr_key,
        to_timestamp('2023-06-22') as run_ts  from insight_dev.ins_bkp.ext_chrg_info
)

select * from charging_info