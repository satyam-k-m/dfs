


with CHRG_INFRMTN as (
     select
		PAY_LINE_NBR,
        TX_NBR,
        TERM_NBR,
        DIV_NBR,
        POS_LOC_ID,
        BIZ_DT,
        ADJ_FLG,
        ACC_NBR,
        ACC_TYPE,
        EXP_DT,
        APP_CD,
        division,
        run_dt,
        surr_key,
        run_ts  from DFS_POC_DB.P_DATA.bronze_chrg_infrmtn

        
            where run_dt = to_date('2023-06-22')
        
)

select * from CHRG_INFRMTN