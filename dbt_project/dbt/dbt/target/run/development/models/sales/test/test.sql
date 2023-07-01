
  create or replace   view external_db.final_sales.test
  
   as (
    select * from external_db.stage.customers
  );

