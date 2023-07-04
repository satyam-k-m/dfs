

with payments as (
    select * from external_db.final_sales.bronze_payments
    
        where run_ts = to_timestamp('2023-06-22')
    
),

final as(
    select 
    payment_id,
    payment_method,
    order_id,
    run_ts

    from payments where status = 'success'
)

select * from final