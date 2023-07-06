


with CHRG_INFRMTN as (
     select
		value:c1:: NUMBER(38,0) AS PAY_LINE_NBR,
        value:c2:: NUMBER(38,0) AS TX_NBR,
        Value:c3:: NUMBER(38,0) AS TERM_NBR,
        value:c4:: NUMBER(38,0) AS DIV_NBR,
        value:c5:: NUMBER(38,0) AS POS_LOC_ID,
        value:c6:: NUMBER(38,0) AS BIZ_DT,
        value:c7:: VARCHAR(1) AS ADJ_FLG,
        value:c8:: NUMBER(19,0) AS ACC_NBR,
        value:c9:: VARCHAR(10) AS ACC_TYPE,
        value:c10:: NUMBER(4,0) AS EXP_DT,
        value:c11:: VARCHAR(10) AS APP_CD,
        division,
        run_dt,
        SHA2_HEX(concat_ws('~',pay_line_nbr,tx_nbr,term_nbr,div_nbr,pos_loc_id)) as surr_key,
        to_timestamp('2023-06-22') as run_ts  from insight_dev.ins_bkp.ext_chrg_info

        
            where run_dt = to_date('2023-06-22')
        
)

select * from CHRG_INFRMTN