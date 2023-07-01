

{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}
with orders as (
    select * from {{ref ('bronze_orders')}}
   {% if is_incremental() %}
        where run_ts = to_timestamp('{{ var("run_ts") }}')
    {% endif %}
),

products as (
    select * from {{ref ('bronze_products')}}
    {% if is_incremental() %}
        where run_ts = to_timestamp('{{ var("run_ts") }}')
    {% endif %}
),

final as (
     select
        orders.order_id,
        orders.customer_id,
        orders.product_id,
        orders.order_date,
        products.price,
        products.run_ts
    from orders
    left join products using (product_id)
)
select * from final
