
{{
    config(
        materialized='incremental',
        unique_key='customer_id'
    )
}}

with order_details as (
    select * from {{ref ('fct_order_detail')}}
    {% if is_incremental() %}
        where run_ts = to_timestamp('{{ var("run_ts") }}')
    {% endif %}
), 
customer as (
    select * from {{ref ('bronze_customers')}}
    {% if is_incremental() %}
        where run_ts = to_timestamp('{{ var("run_ts") }}')
    {% endif %}
),
payments as (
    select * from {{ref ('fct_payment_details')}}
    {% if is_incremental() %}
        where run_ts = to_timestamp('{{ var("run_ts") }}')
    {% endif %}
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
