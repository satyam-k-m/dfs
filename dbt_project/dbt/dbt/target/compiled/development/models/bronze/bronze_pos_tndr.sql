

with POS_TNDR as (
     select
		value:c1:: NUMBER(38,0) AS PAY_LINE_NBR,
		value:c2:: NUMBER(38,0) AS TX_NBR,
		value:c3:: NUMBER(38,0) AS DIV_NBR,
		value:c4:: NUMBER(38,0) AS TERM_NBR ,
		value:c5:: NUMBER(38,0) AS POS_LOC_ID,
		value:c6:: NUMBER(38,0) AS BIZ_DT,
		value:c7:: VARCHAR(1) AS ADJ_FLG,
		value:c8:: VARCHAR(3) AS CURR_CD,
		value:c9:: NUMBER(38,0) AS PAY_MTHD,
		value:c10:: NUMBER(18,4) AS FOR_CURR,
		value:c11:: NUMBER(18,4) AS TEN_AMT,
		value:c12:: NUMBER(18,4) AS EXCH_RATE,
		value:c13:: NUMBER(38,0) AS RCPT_SEQ_NBR,
		value:c14:: VARCHAR(20) AS ADDITION_DATA,
		value:c15:: NUMBER(18,4) AS CHNG_RND,
        division,
        run_dt,
        SHA2_HEX(concat_ws('~',pay_line_nbr,tx_nbr,term_nbr,div_nbr,pos_loc_id)) as surr_key,
        to_timestamp('2023-06-22') as run_ts  from insight_dev.ins_bkp.ext_pos_tndr
)

select * from POS_TNDR