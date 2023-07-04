
{{
    config(
        materialized='incremental',
        unique_key='product_id'

    )
}}

with products as (
     select *, 
     to_timestamp('{{ var("run_ts") }}') as run_ts from {{source('sales_stage','products') }}
)

select * from products