

with TX_TYPE as (
     select
		value:c1::NUMBER(38,0) AS DIV_NBR,
        value:c2::varchar(2) AS APP_CD,
        value:c3::varchar(2) AS TX_TYPE,
        value:c4::varchar(30) AS SHORT_DESC,
        division,
        run_dt,
        SHA2_HEX(concat_ws('~',div_nbr,app_cd,tx_type)) as surr_key,
        to_timestamp('2023-06-22') as run_ts  from insight_dev.ins_bkp.ext_tx_type
)

select * from TX_TYPE