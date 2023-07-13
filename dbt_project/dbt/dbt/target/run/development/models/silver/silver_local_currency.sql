
  create or replace   view DFS_POC_DB.P_DATA.silver_local_currency
  
   as (
    

WITH LOCAL_CURRENCY AS (
    SELECT 
        CURR_CD,
        DIV_NBR,
        CURR_DESC,
        CURR_SHORT_DESC,
        LOCAL_CURR_IND,
        CURR_UNIT,
        DIVISION,
        RUN_DT,
        SURR_KEY,
        RUN_TS
    FROM DFS_POC_DB.P_DATA.bronze_local_currency

    
)
SELECT *
FROM LOCAL_CURRENCY
  );

