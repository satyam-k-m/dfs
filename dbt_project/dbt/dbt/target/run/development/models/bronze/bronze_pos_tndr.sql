-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into insight_dev.INS_BKP.bronze_pos_tndr as DBT_INTERNAL_DEST
        using insight_dev.INS_BKP.bronze_pos_tndr__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.surr_key = DBT_INTERNAL_DEST.surr_key
            )

    
    when matched then update set
        "PAY_LINE_NBR" = DBT_INTERNAL_SOURCE."PAY_LINE_NBR","TX_NBR" = DBT_INTERNAL_SOURCE."TX_NBR","DIV_NBR" = DBT_INTERNAL_SOURCE."DIV_NBR","TERM_NBR" = DBT_INTERNAL_SOURCE."TERM_NBR","POS_LOC_ID" = DBT_INTERNAL_SOURCE."POS_LOC_ID","BIZ_DT" = DBT_INTERNAL_SOURCE."BIZ_DT","ADJ_FLG" = DBT_INTERNAL_SOURCE."ADJ_FLG","CURR_CD" = DBT_INTERNAL_SOURCE."CURR_CD","PAY_MTHD" = DBT_INTERNAL_SOURCE."PAY_MTHD","FOR_CURR" = DBT_INTERNAL_SOURCE."FOR_CURR","TEN_AMT" = DBT_INTERNAL_SOURCE."TEN_AMT","EXCH_RATE" = DBT_INTERNAL_SOURCE."EXCH_RATE","RCPT_SEQ_NBR" = DBT_INTERNAL_SOURCE."RCPT_SEQ_NBR","ADDITION_DATA" = DBT_INTERNAL_SOURCE."ADDITION_DATA","CHNG_RND" = DBT_INTERNAL_SOURCE."CHNG_RND","DIVISION" = DBT_INTERNAL_SOURCE."DIVISION","RUN_DT" = DBT_INTERNAL_SOURCE."RUN_DT","SURR_KEY" = DBT_INTERNAL_SOURCE."SURR_KEY","RUN_TS" = DBT_INTERNAL_SOURCE."RUN_TS"
    

    when not matched then insert
        ("PAY_LINE_NBR", "TX_NBR", "DIV_NBR", "TERM_NBR", "POS_LOC_ID", "BIZ_DT", "ADJ_FLG", "CURR_CD", "PAY_MTHD", "FOR_CURR", "TEN_AMT", "EXCH_RATE", "RCPT_SEQ_NBR", "ADDITION_DATA", "CHNG_RND", "DIVISION", "RUN_DT", "SURR_KEY", "RUN_TS")
    values
        ("PAY_LINE_NBR", "TX_NBR", "DIV_NBR", "TERM_NBR", "POS_LOC_ID", "BIZ_DT", "ADJ_FLG", "CURR_CD", "PAY_MTHD", "FOR_CURR", "TEN_AMT", "EXCH_RATE", "RCPT_SEQ_NBR", "ADDITION_DATA", "CHNG_RND", "DIVISION", "RUN_DT", "SURR_KEY", "RUN_TS")

;
    commit;