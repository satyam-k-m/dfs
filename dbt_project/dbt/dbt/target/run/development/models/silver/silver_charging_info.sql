-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into DFS_POC_DB.P_DATA.silver_charging_info as DBT_INTERNAL_DEST
        using DFS_POC_DB.P_DATA.silver_charging_info__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.surr_key = DBT_INTERNAL_DEST.surr_key
            )

    
    when matched then update set
        "PAY_LINE_NBR" = DBT_INTERNAL_SOURCE."PAY_LINE_NBR","TX_NBR" = DBT_INTERNAL_SOURCE."TX_NBR","TERM_NBR" = DBT_INTERNAL_SOURCE."TERM_NBR","DIV_NBR" = DBT_INTERNAL_SOURCE."DIV_NBR","POS_LOC_ID" = DBT_INTERNAL_SOURCE."POS_LOC_ID","BIZ_DT" = DBT_INTERNAL_SOURCE."BIZ_DT","ADJ_FLG" = DBT_INTERNAL_SOURCE."ADJ_FLG","ACC_NBR" = DBT_INTERNAL_SOURCE."ACC_NBR","ACC_TYPE" = DBT_INTERNAL_SOURCE."ACC_TYPE","EXP_DT" = DBT_INTERNAL_SOURCE."EXP_DT","APP_CD" = DBT_INTERNAL_SOURCE."APP_CD","DIVISION" = DBT_INTERNAL_SOURCE."DIVISION","RUN_DT" = DBT_INTERNAL_SOURCE."RUN_DT","SURR_KEY" = DBT_INTERNAL_SOURCE."SURR_KEY","RUN_TS" = DBT_INTERNAL_SOURCE."RUN_TS"
    

    when not matched then insert
        ("PAY_LINE_NBR", "TX_NBR", "TERM_NBR", "DIV_NBR", "POS_LOC_ID", "BIZ_DT", "ADJ_FLG", "ACC_NBR", "ACC_TYPE", "EXP_DT", "APP_CD", "DIVISION", "RUN_DT", "SURR_KEY", "RUN_TS")
    values
        ("PAY_LINE_NBR", "TX_NBR", "TERM_NBR", "DIV_NBR", "POS_LOC_ID", "BIZ_DT", "ADJ_FLG", "ACC_NBR", "ACC_TYPE", "EXP_DT", "APP_CD", "DIVISION", "RUN_DT", "SURR_KEY", "RUN_TS")

;
    commit;