
  
    

        create or replace transient table external_db.dfs_models.bronze_pos_shop
         as
        (

WITH POS_SHOP AS (
    SELECT 
        VALUE:c1::NUMBER(38, 0) AS POS_LOCATION_ID,
        VALUE:c2::NUMBER(38, 0) AS DIVISON_NUMBER,
        DIVISION,
        RUN_DT,
        SHA2_HEX(CONCAT_WS('~',POS_LOCATION_ID,DIVISON_NUMBER)) AS SURR_KEY,
        TO_TIMESTAMP('2023-06-22') AS RUN_TS
    FROM external_db.stage.ext_pos_shop
)
SELECT *
FROM POS_SHOP
        );
      
  