

with orders as (
     select *, 
     to_timestamp('2023-06-22') as run_ts from external_db.stage.orders
)

select * from orders