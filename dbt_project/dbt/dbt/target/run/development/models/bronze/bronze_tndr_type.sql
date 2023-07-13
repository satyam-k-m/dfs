-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into DFS_POC_DB.P_DATA.bronze_tndr_type as DBT_INTERNAL_DEST
        using DFS_POC_DB.P_DATA.bronze_tndr_type__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.surr_key = DBT_INTERNAL_DEST.surr_key
            )

    
    when matched then update set
        "TEN_TYPE" = DBT_INTERNAL_SOURCE."TEN_TYPE","DIV_NBR" = DBT_INTERNAL_SOURCE."DIV_NBR","SHORT_DESC" = DBT_INTERNAL_SOURCE."SHORT_DESC","LONG_DESC" = DBT_INTERNAL_SOURCE."LONG_DESC","CURR_CARD_TYPE" = DBT_INTERNAL_SOURCE."CURR_CARD_TYPE","SRC_UPDT_DT" = DBT_INTERNAL_SOURCE."SRC_UPDT_DT","DIVISION" = DBT_INTERNAL_SOURCE."DIVISION","RUN_DT" = DBT_INTERNAL_SOURCE."RUN_DT","SURR_KEY" = DBT_INTERNAL_SOURCE."SURR_KEY","RUN_TS" = DBT_INTERNAL_SOURCE."RUN_TS"
    

    when not matched then insert
        ("TEN_TYPE", "DIV_NBR", "SHORT_DESC", "LONG_DESC", "CURR_CARD_TYPE", "SRC_UPDT_DT", "DIVISION", "RUN_DT", "SURR_KEY", "RUN_TS")
    values
        ("TEN_TYPE", "DIV_NBR", "SHORT_DESC", "LONG_DESC", "CURR_CARD_TYPE", "SRC_UPDT_DT", "DIVISION", "RUN_DT", "SURR_KEY", "RUN_TS")

;
    commit;