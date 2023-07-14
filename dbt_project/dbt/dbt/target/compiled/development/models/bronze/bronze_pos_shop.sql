


with POS_SHOP as (
    select * exclude rn from (
     select
		value:c1:: NUMBER(38,0) AS POS_LOCATION_ID,
		value:c2:: NUMBER(38,0) AS DIVISON_NUMBER,
        SHA2_HEX(concat_ws('~',pos_location_id,divison_number)) as bsns_key, --open to discussion if we need hex calculation -> move to HASH
        20230622110040 as run_id,
        row_number() over(partition by pos_location_id,divison_number order by 1) as rn

        from insight_dev.ins_bkp.ext_pos_shop
        where
        division =  'MAC'
        and run_dt = substr('20230622110040',0,8)
    )
    where rn =1
)

select * from POS_SHOP