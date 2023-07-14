
{{
    config(
        materialized='table'
    )
}}


with POS_SHOP as (
    select * exclude rn from (
     select
		value:c1:: NUMBER(38,0) AS POS_LOCATION_ID,
		value:c2:: NUMBER(38,0) AS DIVISON_NUMBER,
        SHA2_HEX(concat_ws('~',pos_location_id,divison_number)) as bsns_key, --open to discussion if we need hex calculation -> move to HASH
        {{ var("run_id") }} as run_id,
        row_number() over(partition by pos_location_id,divison_number order by 1) as rn

        from {{source('dfs_stage','ext_pos_shop') }}
        where
        division =  '{{ var("division") }}'
        and run_dt = substr('{{ var("run_id") }}',0,8)
    )
    where rn =1
)

select * from POS_SHOP
