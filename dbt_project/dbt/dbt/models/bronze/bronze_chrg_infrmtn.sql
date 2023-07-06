
{{
    config(
        materialized='incremental',
        unique_key='surr_key'

    )
}}


with CHRG_INFRMTN as (
     select
		value:c1:: NUMBER(38,0) AS PAY_LINE_NBR,
        value:c2:: NUMBER(38,0) AS TX_NBR,
        Value:c3:: NUMBER(38,0) AS TERM_NBR,
        value:c4:: NUMBER(38,0) AS DIV_NBR,
        value:c5:: NUMBER(38,0) AS POS_LOC_ID,
        value:c6:: NUMBER(38,0) AS BIZ_DT,
        value:c7:: VARCHAR(1) AS ADJ_FLG,
        value:c8:: NUMBER(19,0) AS ACC_NBR,
        value:c9:: VARCHAR(10) AS ACC_TYPE,
        value:c10:: NUMBER(4,0) AS EXP_DT,
        value:c11:: VARCHAR(10) AS APP_CD,
        division,
        run_dt,
        SHA2_HEX(concat_ws('~',pay_line_nbr,tx_nbr,term_nbr,div_nbr,pos_loc_id)) as surr_key,
        to_timestamp('{{ var("run_ts") }}') as run_ts  from {{source('dfs_stage','ext_chrg_info') }}

        {% if is_incremental() %}
            where run_dt = to_date('{{ var("run_ts") }}')
        {% endif %}
)

select * from CHRG_INFRMTN
