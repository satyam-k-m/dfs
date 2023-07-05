

with TNDR_TYPE as (
     select
		value:c1::NUMBER(38,0) AS TEN_TYPE,
        value:c2::NUMBER(38,0) AS DIV_NBR,
        value:c3::varchar(4) AS SHORT_DESC,
        value:c4::varchar(30) AS LONG_DESC,
        value:c5::varchar(1) AS CURR_CARD_TYPE,
        value:c6::NUMBER(38,0) AS SRC_UPDT_DT,
        division,
        run_dt,
        SHA2_HEX(concat_ws('~',ten_type,div_nbr)) as surr_key,
        to_timestamp('2023-06-22') as run_ts  from insight_dev.ins_bkp.ext_tndr_type
)

select * from TNDR_TYPE