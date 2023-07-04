

with customers as (
     select *, 
     to_timestamp('2023-06-22') as run_ts  from external_db.stage.customers
)

select * from customers