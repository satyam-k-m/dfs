-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into external_db.final_sales.bronze_payments as DBT_INTERNAL_DEST
        using external_db.final_sales.bronze_payments__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.payment_id = DBT_INTERNAL_DEST.payment_id
            )

    
    when matched then update set
        "VALUE" = DBT_INTERNAL_SOURCE."VALUE","PAYMENT_ID" = DBT_INTERNAL_SOURCE."PAYMENT_ID","ORDER_ID" = DBT_INTERNAL_SOURCE."ORDER_ID","PAYMENT_METHOD" = DBT_INTERNAL_SOURCE."PAYMENT_METHOD","STATUS" = DBT_INTERNAL_SOURCE."STATUS","RUN_TS" = DBT_INTERNAL_SOURCE."RUN_TS"
    

    when not matched then insert
        ("VALUE", "PAYMENT_ID", "ORDER_ID", "PAYMENT_METHOD", "STATUS", "RUN_TS")
    values
        ("VALUE", "PAYMENT_ID", "ORDER_ID", "PAYMENT_METHOD", "STATUS", "RUN_TS")

;
    commit;