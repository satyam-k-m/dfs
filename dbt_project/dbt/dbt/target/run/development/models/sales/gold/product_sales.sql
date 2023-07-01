-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into external_db.final_sales.product_sales as DBT_INTERNAL_DEST
        using external_db.final_sales.product_sales__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.customer_id = DBT_INTERNAL_DEST.customer_id
            )

    
    when matched then update set
        "CUSTOMER_ID" = DBT_INTERNAL_SOURCE."CUSTOMER_ID","ORDER_AMOUNT" = DBT_INTERNAL_SOURCE."ORDER_AMOUNT"
    

    when not matched then insert
        ("CUSTOMER_ID", "ORDER_AMOUNT")
    values
        ("CUSTOMER_ID", "ORDER_AMOUNT")

;
    commit;