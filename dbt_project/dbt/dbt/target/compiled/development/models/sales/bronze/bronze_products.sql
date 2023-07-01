

with products as (
     select *, 
     to_timestamp('2023-06-22') as run_ts from external_db.stage.products
)

select * from products