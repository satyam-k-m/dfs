
{{
    config(
        materialized='incremental',
        unique_key='payment_id'
    )
}}

with payments as (
     select *, 
     to_timestamp('{{ var("run_ts") }}') as run_ts from {{source('sales_stage','payments') }}
)

select * from payments