
{{
    config(
        materialized='incremental',
        unique_key='order_id'

    )
}}

with orders as (
     select *, 
     to_timestamp('{{ var("run_ts") }}') as run_ts from {{source('sales_stage','orders') }}
)

select * from orders 