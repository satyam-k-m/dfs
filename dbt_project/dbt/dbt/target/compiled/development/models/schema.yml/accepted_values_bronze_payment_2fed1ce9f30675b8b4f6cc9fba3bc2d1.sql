
    
    

with all_values as (

    select
        payment_method as value_field,
        count(*) as n_records

    from external_db.final_sales.bronze_payments
    group by payment_method

)

select *
from all_values
where value_field not in (
    'debit','credit','upi','cash'
)


