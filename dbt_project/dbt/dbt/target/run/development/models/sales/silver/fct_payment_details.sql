-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into external_db.final_sales.fct_payment_details as DBT_INTERNAL_DEST
        using external_db.final_sales.fct_payment_details__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.payment_id = DBT_INTERNAL_DEST.payment_id
            )

    
    when matched then update set
        "PAYMENT_ID" = DBT_INTERNAL_SOURCE."PAYMENT_ID","PAYMENT_METHOD" = DBT_INTERNAL_SOURCE."PAYMENT_METHOD","ORDER_ID" = DBT_INTERNAL_SOURCE."ORDER_ID","RUN_TS" = DBT_INTERNAL_SOURCE."RUN_TS"
    

    when not matched then insert
        ("PAYMENT_ID", "PAYMENT_METHOD", "ORDER_ID", "RUN_TS")
    values
        ("PAYMENT_ID", "PAYMENT_METHOD", "ORDER_ID", "RUN_TS")

;
    commit;