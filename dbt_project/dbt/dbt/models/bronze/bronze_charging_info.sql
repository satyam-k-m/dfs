
{{
    config(
        materialized='incremental',
        unique_key='customer_id'

    )
}}

with customers as (
     select *, 
     to_timestamp('{{ var("run_ts") }}') as run_ts  from {{source('sales_stage','customers') }}
)

select * from customers
