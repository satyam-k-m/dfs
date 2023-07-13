

with POS_SHOP as (
     select
		POS_LOCATION_ID,
		DIVISON_NUMBER,
        division,
        run_dt,
        surr_key,
        run_ts  from DFS_POC_DB.P_DATA.bronze_pos_shop

        
            where run_dt = to_date('2023-06-22')
        
)

select * from POS_SHOP