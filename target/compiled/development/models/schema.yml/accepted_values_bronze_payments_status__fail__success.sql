
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from external_db.final_sales.bronze_payments
    group by status

)

select *
from all_values
where value_field not in (
    'fail','success'
)


