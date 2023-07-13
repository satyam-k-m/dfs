

with division as (
     select
        DIV_NBR,
        DIV_NAME, 
        CITY,
        STATE,
        ZIP_CD,
        DVSN_RPT_HDNG,
        TRAD_COMP_CD,
        DB_COST_FLG,
        CREATE_PREORD_TRANS_AUD_REC,
        POPI_CNT_FLG,
        RCENT_CD,
        AUTO_WRITE_OFF_LIMIT,
        MRPU_ENTRY,
        SCLS_REALIGN_DUR_PI,
        division,
        run_dt,
        surr_key,
        run_ts  from DFS_POC_DB.P_DATA.bronze_divison

        
)

select * from division