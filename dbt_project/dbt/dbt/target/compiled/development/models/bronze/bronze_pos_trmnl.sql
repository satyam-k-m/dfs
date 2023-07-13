

with POS_TRMNL as (
     select
		value:c1::NUMBER(38,0) AS POS_LOCATION_ID,
		value:c2::NUMBER(38,0) AS DIVISON_NUMBER,
		value:c3::NUMBER(38,0) AS TERMINAL_NUMBER,
        division,
        run_dt,
        SHA2_HEX(concat_ws('~',pos_location_id,divison_number,terminal_number)) as surr_key,
        to_timestamp('2023-06-22') as run_ts  from DFS_POC_DB.P_DATA.ext_pos_trmnl

        
            where run_dt = to_date('2023-06-22')
        
)

select * from POS_TRMNL