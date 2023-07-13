-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into DFS_POC_DB.P_DATA.bronze_pos_tx as DBT_INTERNAL_DEST
        using DFS_POC_DB.P_DATA.bronze_pos_tx__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.surr_key = DBT_INTERNAL_DEST.surr_key
            )

    
    when matched then update set
        "TX_NBR" = DBT_INTERNAL_SOURCE."TX_NBR","POS_LOC_ID" = DBT_INTERNAL_SOURCE."POS_LOC_ID","DIV_NBR" = DBT_INTERNAL_SOURCE."DIV_NBR","TERM_NBR" = DBT_INTERNAL_SOURCE."TERM_NBR","BIZ_DT" = DBT_INTERNAL_SOURCE."BIZ_DT","ADJ_FLG" = DBT_INTERNAL_SOURCE."ADJ_FLG","TX_DT" = DBT_INTERNAL_SOURCE."TX_DT","TX_TIME" = DBT_INTERNAL_SOURCE."TX_TIME","CURR_CD" = DBT_INTERNAL_SOURCE."CURR_CD","POS_TX_TYPE" = DBT_INTERNAL_SOURCE."POS_TX_TYPE","MERCH_TX_TYPE" = DBT_INTERNAL_SOURCE."MERCH_TX_TYPE","CUST_ID" = DBT_INTERNAL_SOURCE."CUST_ID","LOCAL_NAT_CD" = DBT_INTERNAL_SOURCE."LOCAL_NAT_CD","REKEY_IND" = DBT_INTERNAL_SOURCE."REKEY_IND","ITEM_CNT" = DBT_INTERNAL_SOURCE."ITEM_CNT","CASHIER_NBR" = DBT_INTERNAL_SOURCE."CASHIER_NBR","LOCAL_NET_AMT" = DBT_INTERNAL_SOURCE."LOCAL_NET_AMT","LOCAL_SUBTOT" = DBT_INTERNAL_SOURCE."LOCAL_SUBTOT","LOCAL_TOT_TAX" = DBT_INTERNAL_SOURCE."LOCAL_TOT_TAX","TOT_PAY_AMT" = DBT_INTERNAL_SOURCE."TOT_PAY_AMT","HOST_CUR_CD" = DBT_INTERNAL_SOURCE."HOST_CUR_CD","HOST_NET_AMT" = DBT_INTERNAL_SOURCE."HOST_NET_AMT","HOST_SUBTOT" = DBT_INTERNAL_SOURCE."HOST_SUBTOT","HOST_TOT_TAX" = DBT_INTERNAL_SOURCE."HOST_TOT_TAX","RFND_REAS" = DBT_INTERNAL_SOURCE."RFND_REAS","DUTY_PAY_FLG" = DBT_INTERNAL_SOURCE."DUTY_PAY_FLG","MEMBERSHIP_CARD_NUMBER" = DBT_INTERNAL_SOURCE."MEMBERSHIP_CARD_NUMBER","CUST_FNAME" = DBT_INTERNAL_SOURCE."CUST_FNAME","CUST_LNAME" = DBT_INTERNAL_SOURCE."CUST_LNAME","CUST_GNDR" = DBT_INTERNAL_SOURCE."CUST_GNDR","CUST_PSPRT" = DBT_INTERNAL_SOURCE."CUST_PSPRT","CUST_FLT" = DBT_INTERNAL_SOURCE."CUST_FLT","CUST_DEPDATE" = DBT_INTERNAL_SOURCE."CUST_DEPDATE","FLT_DST" = DBT_INTERNAL_SOURCE."FLT_DST","FLT_CLSS" = DBT_INTERNAL_SOURCE."FLT_CLSS","FLT_DPNT" = DBT_INTERNAL_SOURCE."FLT_DPNT","FLT_SEAT" = DBT_INTERNAL_SOURCE."FLT_SEAT","FLT_CFLT" = DBT_INTERNAL_SOURCE."FLT_CFLT","FLT_CDEPDATE" = DBT_INTERNAL_SOURCE."FLT_CDEPDATE","FLT_CDST" = DBT_INTERNAL_SOURCE."FLT_CDST","ID_TYPE" = DBT_INTERNAL_SOURCE."ID_TYPE","ID_NO" = DBT_INTERNAL_SOURCE."ID_NO","DIVISION" = DBT_INTERNAL_SOURCE."DIVISION","RUN_DT" = DBT_INTERNAL_SOURCE."RUN_DT","SURR_KEY" = DBT_INTERNAL_SOURCE."SURR_KEY","RUN_TS" = DBT_INTERNAL_SOURCE."RUN_TS"
    

    when not matched then insert
        ("TX_NBR", "POS_LOC_ID", "DIV_NBR", "TERM_NBR", "BIZ_DT", "ADJ_FLG", "TX_DT", "TX_TIME", "CURR_CD", "POS_TX_TYPE", "MERCH_TX_TYPE", "CUST_ID", "LOCAL_NAT_CD", "REKEY_IND", "ITEM_CNT", "CASHIER_NBR", "LOCAL_NET_AMT", "LOCAL_SUBTOT", "LOCAL_TOT_TAX", "TOT_PAY_AMT", "HOST_CUR_CD", "HOST_NET_AMT", "HOST_SUBTOT", "HOST_TOT_TAX", "RFND_REAS", "DUTY_PAY_FLG", "MEMBERSHIP_CARD_NUMBER", "CUST_FNAME", "CUST_LNAME", "CUST_GNDR", "CUST_PSPRT", "CUST_FLT", "CUST_DEPDATE", "FLT_DST", "FLT_CLSS", "FLT_DPNT", "FLT_SEAT", "FLT_CFLT", "FLT_CDEPDATE", "FLT_CDST", "ID_TYPE", "ID_NO", "DIVISION", "RUN_DT", "SURR_KEY", "RUN_TS")
    values
        ("TX_NBR", "POS_LOC_ID", "DIV_NBR", "TERM_NBR", "BIZ_DT", "ADJ_FLG", "TX_DT", "TX_TIME", "CURR_CD", "POS_TX_TYPE", "MERCH_TX_TYPE", "CUST_ID", "LOCAL_NAT_CD", "REKEY_IND", "ITEM_CNT", "CASHIER_NBR", "LOCAL_NET_AMT", "LOCAL_SUBTOT", "LOCAL_TOT_TAX", "TOT_PAY_AMT", "HOST_CUR_CD", "HOST_NET_AMT", "HOST_SUBTOT", "HOST_TOT_TAX", "RFND_REAS", "DUTY_PAY_FLG", "MEMBERSHIP_CARD_NUMBER", "CUST_FNAME", "CUST_LNAME", "CUST_GNDR", "CUST_PSPRT", "CUST_FLT", "CUST_DEPDATE", "FLT_DST", "FLT_CLSS", "FLT_DPNT", "FLT_SEAT", "FLT_CFLT", "FLT_CDEPDATE", "FLT_CDST", "ID_TYPE", "ID_NO", "DIVISION", "RUN_DT", "SURR_KEY", "RUN_TS")

;
    commit;