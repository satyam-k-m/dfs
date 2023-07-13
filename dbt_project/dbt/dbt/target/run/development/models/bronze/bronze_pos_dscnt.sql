-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into DFS_POC_DB.P_DATA.bronze_pos_dscnt as DBT_INTERNAL_DEST
        using DFS_POC_DB.P_DATA.bronze_pos_dscnt__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.surr_key = DBT_INTERNAL_DEST.surr_key
            )

    
    when matched then update set
        "DIV_DSCNT_CD" = DBT_INTERNAL_SOURCE."DIV_DSCNT_CD","DIV_NBR" = DBT_INTERNAL_SOURCE."DIV_NBR","DIV_DSCNT_DESC" = DBT_INTERNAL_SOURCE."DIV_DSCNT_DESC","MERCH_DSCNT_CD" = DBT_INTERNAL_SOURCE."MERCH_DSCNT_CD","MD_DSCNT_CD" = DBT_INTERNAL_SOURCE."MD_DSCNT_CD","DIVISION" = DBT_INTERNAL_SOURCE."DIVISION","RUN_DT" = DBT_INTERNAL_SOURCE."RUN_DT","SURR_KEY" = DBT_INTERNAL_SOURCE."SURR_KEY","RUN_TS" = DBT_INTERNAL_SOURCE."RUN_TS"
    

    when not matched then insert
        ("DIV_DSCNT_CD", "DIV_NBR", "DIV_DSCNT_DESC", "MERCH_DSCNT_CD", "MD_DSCNT_CD", "DIVISION", "RUN_DT", "SURR_KEY", "RUN_TS")
    values
        ("DIV_DSCNT_CD", "DIV_NBR", "DIV_DSCNT_DESC", "MERCH_DSCNT_CD", "MD_DSCNT_CD", "DIVISION", "RUN_DT", "SURR_KEY", "RUN_TS")

;
    commit;