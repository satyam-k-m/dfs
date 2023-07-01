
{{
    config(
        materialized='incremental',
        unique_key='payment_id'
    )
}}

with payments as (
    select * from {{ref ('bronze_payments')}}
    {% if is_incremental() %}
        where run_ts = to_timestamp('{{ var("run_ts") }}')
    {% endif %}
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
