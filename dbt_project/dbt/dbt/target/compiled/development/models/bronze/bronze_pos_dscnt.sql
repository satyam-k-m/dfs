

with POS_DSCNT as (
     select
		value:c1:: NUMBER(38,0) AS DIV_NBR,
		value:c2:: VARCHAR(2) AS DIV_DSCNT_CD,
		value:c3:: VARCHAR(30) AS DIV_DSCNT_DESC,
		value:c4:: NUMBER(2,0) AS MERCH_DSCNT_CD,
		value:c5:: VARCHAR(1) AS MD_DSCNT_CD,
        division,
        run_dt,
        SHA2_HEX(concat_ws('~',div_nbr,div_dscnt_cd)) as surr_key,
        to_timestamp('2023-06-22') as run_ts  from insight_dev.ins_bkp.ext_pos_tx_dct
)

select * from POS_DSCNT