-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into external_db.final_sales.bronze_customers as DBT_INTERNAL_DEST
        using external_db.final_sales.bronze_customers__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.customer_id = DBT_INTERNAL_DEST.customer_id
            )

    
    when matched then update set
        "VALUE" = DBT_INTERNAL_SOURCE."VALUE","CUSTOMER_ID" = DBT_INTERNAL_SOURCE."CUSTOMER_ID","FIRST_NAME" = DBT_INTERNAL_SOURCE."FIRST_NAME","LAST_NAME" = DBT_INTERNAL_SOURCE."LAST_NAME","RUN_TS" = DBT_INTERNAL_SOURCE."RUN_TS"
    

    when not matched then insert
        ("VALUE", "CUSTOMER_ID", "FIRST_NAME", "LAST_NAME", "RUN_TS")
    values
        ("VALUE", "CUSTOMER_ID", "FIRST_NAME", "LAST_NAME", "RUN_TS")

;
    commit;