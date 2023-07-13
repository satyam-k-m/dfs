-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into DFS_POC_DB.P_DATA.silver_pos_terminal as DBT_INTERNAL_DEST
        using DFS_POC_DB.P_DATA.silver_pos_terminal__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.surr_key = DBT_INTERNAL_DEST.surr_key
            )

    
    when matched then update set
        "POS_LOCATION_ID" = DBT_INTERNAL_SOURCE."POS_LOCATION_ID","DIVISON_NUMBER" = DBT_INTERNAL_SOURCE."DIVISON_NUMBER","TERMINAL_NUMBER" = DBT_INTERNAL_SOURCE."TERMINAL_NUMBER","DIVISION" = DBT_INTERNAL_SOURCE."DIVISION","RUN_DT" = DBT_INTERNAL_SOURCE."RUN_DT","SURR_KEY" = DBT_INTERNAL_SOURCE."SURR_KEY","RUN_TS" = DBT_INTERNAL_SOURCE."RUN_TS"
    

    when not matched then insert
        ("POS_LOCATION_ID", "DIVISON_NUMBER", "TERMINAL_NUMBER", "DIVISION", "RUN_DT", "SURR_KEY", "RUN_TS")
    values
        ("POS_LOCATION_ID", "DIVISON_NUMBER", "TERMINAL_NUMBER", "DIVISION", "RUN_DT", "SURR_KEY", "RUN_TS")

;
    commit;