-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into insight_dev.INS_BKP.pos_shop as DBT_INTERNAL_DEST
        using insight_dev.INS_BKP.pos_shop__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.bsns_key = DBT_INTERNAL_DEST.bsns_key
            )

    
    when matched then update set
        "POC_LCTN_ID" = DBT_INTERNAL_SOURCE."POC_LCTN_ID","DVSN_NBR" = DBT_INTERNAL_SOURCE."DVSN_NBR","BSNS_KEY" = DBT_INTERNAL_SOURCE."BSNS_KEY","RUN_ID" = DBT_INTERNAL_SOURCE."RUN_ID","DVSN_SRRGT_KEY" = DBT_INTERNAL_SOURCE."DVSN_SRRGT_KEY","IS_SUSPENDED" = DBT_INTERNAL_SOURCE."IS_SUSPENDED","SPNS_RVRSL_TIMESTAMP" = DBT_INTERNAL_SOURCE."SPNS_RVRSL_TIMESTAMP"
    

    when not matched then insert
        ("POC_LCTN_ID", "DVSN_NBR", "BSNS_KEY", "RUN_ID", "DVSN_SRRGT_KEY", "IS_SUSPENDED", "SPNS_RVRSL_TIMESTAMP")
    values
        ("POC_LCTN_ID", "DVSN_NBR", "BSNS_KEY", "RUN_ID", "DVSN_SRRGT_KEY", "IS_SUSPENDED", "SPNS_RVRSL_TIMESTAMP")

;
    commit;