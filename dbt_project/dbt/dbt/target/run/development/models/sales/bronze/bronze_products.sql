-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into external_db.final_sales.bronze_products as DBT_INTERNAL_DEST
        using external_db.final_sales.bronze_products__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.product_id = DBT_INTERNAL_DEST.product_id
            )

    
    when matched then update set
        "VALUE" = DBT_INTERNAL_SOURCE."VALUE","PRODUCT_ID" = DBT_INTERNAL_SOURCE."PRODUCT_ID","PRICE" = DBT_INTERNAL_SOURCE."PRICE","RUN_TS" = DBT_INTERNAL_SOURCE."RUN_TS"
    

    when not matched then insert
        ("VALUE", "PRODUCT_ID", "PRICE", "RUN_TS")
    values
        ("VALUE", "PRODUCT_ID", "PRICE", "RUN_TS")

;
    commit;