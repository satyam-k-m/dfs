

with division as (
     select
        value:c1:: NUMBER(38,0) AS DIV_NBR,
        value:c2:: VARCHAR(25) AS DIV_NAME, 
        value:c3:: VARCHAR(30) AS ADDR_LINE_1, 
        value:c4:: VARCHAR(30) AS ADDR_LINE_2, 
        value:c5:: VARCHAR(30) AS ADDR_LINE_3,
        value:c6:: VARCHAR(20) AS CITY,
        value:c7:: VARCHAR(2) AS STATE,
        value:c8:: VARCHAR(9) AS ZIP_CD,
        value:c9:: VARCHAR(30) AS DVSN_RPT_HDNG,
        value:c10:: VARCHAR(20) AS WRITE_OFF_DESC_1,
        value:c11:: VARCHAR(20) AS WRIRE_OFF_DESC_2,
        value:c12:: VARCHAR(1) AS DT_FORMAT_CD,
        value:c13:: NUMBER(38,0) AS SALES_CTL_NBR,
        value:c14:: NUMBER(38,0) AS CNCL_PO_DAYS,
        value:c15:: NUMBER(38,0) AS CNCL_PO_DAYS_PCT,
        value:c16:: VARCHAR(3) AS CURR_CD,
        value:c17:: VARCHAR(3) AS MULT_OTB_PRES,
        value:c18:: VARCHAR(25) AS MULT_UNIT_RTL,
        value:c19:: VARCHAR(1) AS MULT_DEPT_IN_PO,
        value:c20:: NUMBER(38,0) AS MOD_CHK_ROUTINE,
        value:c21:: NUMBER(38,0) AS INV_VAR_PCT,
        value:c22:: NUMBER(18,4) AS INV_VAR_AMT,
        value:c23:: VARCHAR(1) AS COST_REAVG,
        value:c24:: VARCHAR(1) AS SALES_PROCESS_FLG,
        value:c25:: NUMBER(18,4) AS INV_MIN_CHGBCK_AMT,
        value:c26:: VARCHAR(1) AS CUSTOMS_FLG,
        value:c27:: NUMBER(38,0) AS EXCH_RATE_VAR,
        value:c28:: NUMBER(38,0) AS INV_CUTOFF_DAY,
        value:c29:: VARCHAR(1) AS PLAN_LVL,
        value:c30:: VARCHAR(1) AS PRICE_LKUP_FLG,
        value:c31:: VARCHAR(1) AS TRAD_COMP_CD,
        value:c32:: VARCHAR(1) AS DB_COST_FLG,
        value:c33:: VARCHAR(1) AS CREATE_PREORD_TRANS_AUD_REC,
        value:c34:: VARCHAR(1) AS POPI_CNT_FLG,
        value:c35:: VARCHAR(1) AS RCENT_CD,
        value:c36:: NUMBER(7,2) AS AUTO_WRITE_OFF_LIMIT,
        value:c37:: VARCHAR(1) AS MRPU_ENTRY,
        value:c38:: VARCHAR(1) AS SCLS_REALIGN_DUR_PI,
        division,
        run_dt,
        SHA2_HEX(DIV_NBR) as surr_key,
        to_timestamp('2023-06-22') as run_ts  from DFS_POC_DB.P_DATA.ext_divison

        
)

select * from division