-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into insight_dev.INS_BKP.bronze_tx_type as DBT_INTERNAL_DEST
        using insight_dev.INS_BKP.bronze_tx_type__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.surr_key = DBT_INTERNAL_DEST.surr_key
            )

    
    when matched then update set
        "DIV_NBR" = DBT_INTERNAL_SOURCE."DIV_NBR","APP_CD" = DBT_INTERNAL_SOURCE."APP_CD","TX_TYPE" = DBT_INTERNAL_SOURCE."TX_TYPE","SHORT_DESC" = DBT_INTERNAL_SOURCE."SHORT_DESC","DIVISION" = DBT_INTERNAL_SOURCE."DIVISION","RUN_DT" = DBT_INTERNAL_SOURCE."RUN_DT","SURR_KEY" = DBT_INTERNAL_SOURCE."SURR_KEY","RUN_TS" = DBT_INTERNAL_SOURCE."RUN_TS"
    

    when not matched then insert
        ("DIV_NBR", "APP_CD", "TX_TYPE", "SHORT_DESC", "DIVISION", "RUN_DT", "SURR_KEY", "RUN_TS")
    values
        ("DIV_NBR", "APP_CD", "TX_TYPE", "SHORT_DESC", "DIVISION", "RUN_DT", "SURR_KEY", "RUN_TS")

;
    commit;