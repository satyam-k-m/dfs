

with order_details as (
    select * from external_db.final_sales.fct_order_detail
    
        where run_ts = to_timestamp('2023-06-22')
    
), 
customer as (
    select * from external_db.final_sales.bronze_customers
    
        where run_ts = to_timestamp('2023-06-22')
    
),
payments as (
    select * from external_db.final_sales.fct_payment_details
    
        where run_ts = to_timestamp('2023-06-22')
    
),

final as (

    select
        order_details.customer_id,
        sum(order_details.price) as order_amount

    from order_details
    left join customer using (customer_id)

    left join payments using (order_id)

    group by 1
)
select * from final