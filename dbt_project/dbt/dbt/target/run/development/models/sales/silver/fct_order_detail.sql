-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into external_db.final_sales.fct_order_detail as DBT_INTERNAL_DEST
        using external_db.final_sales.fct_order_detail__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.order_id = DBT_INTERNAL_DEST.order_id
            )

    
    when matched then update set
        "ORDER_ID" = DBT_INTERNAL_SOURCE."ORDER_ID","CUSTOMER_ID" = DBT_INTERNAL_SOURCE."CUSTOMER_ID","PRODUCT_ID" = DBT_INTERNAL_SOURCE."PRODUCT_ID","ORDER_DATE" = DBT_INTERNAL_SOURCE."ORDER_DATE","PRICE" = DBT_INTERNAL_SOURCE."PRICE","RUN_TS" = DBT_INTERNAL_SOURCE."RUN_TS"
    

    when not matched then insert
        ("ORDER_ID", "CUSTOMER_ID", "PRODUCT_ID", "ORDER_DATE", "PRICE", "RUN_TS")
    values
        ("ORDER_ID", "CUSTOMER_ID", "PRODUCT_ID", "ORDER_DATE", "PRICE", "RUN_TS")

;
    commit;