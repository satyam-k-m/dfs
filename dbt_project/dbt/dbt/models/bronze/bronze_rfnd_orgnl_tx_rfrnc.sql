
{{
    config(
        materialized='incremental',
        unique_key='surr_key'

    )
}}

with RFND_ORGNL_TX_RFRNC as (
     select
		ORIG_TX_NBR NUMBER(38,0) AS (value:c1::NUMBER(38,0)),
		ORIG_TERM_NBR NUMBER(38,0) AS (value:c2::NUMBER(38,0)),
		ORIG_DIV_NBR NUMBER(38,0) AS (value:c3::NUMBER(38,0)),
		ORIG_POS_LOC_ID NUMBER(38,0) AS (value:c4::NUMBER(38,0)),
		ORIG_BIZ_DT NUMBER(38,0) AS (value:c5::NUMBER(38,0)),
		ORIG_ADJ_FLG VARCHAR(1) AS (value:c6::varchar(1)),
		ORIG_TX_DT NUMBER(38,0) AS (value:c7::NUMBER(38,0)),
		RFND_TX_NBR NUMBER(38,0) AS (value:c8::NUMBER(38,0)),
		RFND_TERM_NBR NUMBER(38,0) AS (value:c9::NUMBER(38,0)),
		RFND_DIV_NBR NUMBER(38,0) AS (value:c10::NUMBER(38,0)),
		RFND_POS_LOC_ID NUMBER(38,0) AS (value:c11::NUMBER(38,0)),
		RFND_BIZ_DT NUMBER(38,0) AS (value:c12::NUMBER(38,0)),
		RFND_ADJ_FLG VARCHAR(1) AS (value:c13::varchar(1)),
		RFND_TX_DT NUMBER(38,0) AS (value:c14::NUMBER(38,0)),
        division,
        run_dt,
        SHA2_HEX(concat_ws('~',ORIG_TX_NBR,ORIG_TERM_NBR)) as surr_key,
        to_timestamp('{{ var("run_ts") }}') as run_ts  from {{source('dfs_stage','ext_RFND_ORGNL_TX_RFRNC') }}
)

select * from RFND_ORGNL_TX_RFRNC