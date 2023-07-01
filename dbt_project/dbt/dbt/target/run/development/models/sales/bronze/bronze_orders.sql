-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into external_db.final_sales.bronze_orders as DBT_INTERNAL_DEST
        using external_db.final_sales.bronze_orders__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.order_id = DBT_INTERNAL_DEST.order_id
            )

    
    when matched then update set
        "VALUE" = DBT_INTERNAL_SOURCE."VALUE","ORDER_ID" = DBT_INTERNAL_SOURCE."ORDER_ID","CUSTOMER_ID" = DBT_INTERNAL_SOURCE."CUSTOMER_ID","PRODUCT_ID" = DBT_INTERNAL_SOURCE."PRODUCT_ID","ORDER_DATE" = DBT_INTERNAL_SOURCE."ORDER_DATE","RUN_TS" = DBT_INTERNAL_SOURCE."RUN_TS"
    

    when not matched then insert
        ("VALUE", "ORDER_ID", "CUSTOMER_ID", "PRODUCT_ID", "ORDER_DATE", "RUN_TS")
    values
        ("VALUE", "ORDER_ID", "CUSTOMER_ID", "PRODUCT_ID", "ORDER_DATE", "RUN_TS")

;
    commit;