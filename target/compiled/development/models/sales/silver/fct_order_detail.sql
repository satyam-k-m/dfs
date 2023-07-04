
with orders as (
    select * from external_db.final_sales.bronze_orders
   
        where run_ts = to_timestamp('2023-06-22')
    
),

products as (
    select * from external_db.final_sales.bronze_products
    
        where run_ts = to_timestamp('2023-06-22')
    
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