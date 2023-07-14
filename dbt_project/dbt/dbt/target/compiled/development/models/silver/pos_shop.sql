

with delta as (
    select
            
            brz.POS_LOCATION_ID as poc_lctn_id,
            brz.DIVISON_NUMBER as dvsn_nbr,
            brz.bsns_key as bsns_key,
            brz.run_id as run_id,
            nvl(dvsn.dvsn_srrgt_key,'-1') as dvsn_srrgt_key,
            case when nvl(dvsn.dvsn_srrgt_key,'-1') = '-1' then true else false end as is_suspended ,
            null as spns_rvrsl_timestamp
    from
            insight_dev.INS_BKP.bronze_pos_shop brz
    LEFT OUTER JOIN insight_dev.ins_bkp.DVSN dvsn
            on brz.DIVISON_NUMBER=dvsn.dvsn_nbr
            

    union all

    select       
            slv.poc_lctn_id,
            slv.dvsn_nbr,
            slv.bsns_key as bsns_key,
            slv.run_id as run_id,
            dvsn.dvsn_srrgt_key as dvsn_srrgt_key,
            false as is_suspended,
            current_timestamp as spns_rvrsl_timestamp
    from
            insight_dev.INS_BKP.pos_shop slv
    LEFT OUTER JOIN insight_dev.ins_bkp.DVSN dvsn
            on slv.dvsn_nbr=dvsn.dvsn_nbr
    where slv.is_suspended = true
    and dvsn.dvsn_srrgt_key is not null

),

ranked_dataset as (
    select *,
    row_number() over (partition by bsns_key order by run_id desc) as rn 
    from
    delta
)

select * exclude rn from ranked_dataset where rn = 1