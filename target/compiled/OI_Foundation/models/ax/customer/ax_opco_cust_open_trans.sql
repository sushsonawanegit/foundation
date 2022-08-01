

with  __dbt__cte__v_ax_opco_cust_trans_curr as (
with v_ax_opco_cust_trans as(
    select *,
    rank() over(partition by opco_cust_trans_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_trans
),
final as(
    select 
    *
    from v_ax_opco_cust_trans
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_curr as (
with v_ax_opco as(
    select *,
    rank() over(partition by opco_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco
),
final as(
    select 
    *
    from v_ax_opco
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_cust_curr as (
with v_ax_opco_cust as(
    select *,
    rank() over(partition by opco_cust_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust
),
final as(
    select 
    *
    from v_ax_opco_cust
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_chart_of_accts_curr as (
with v_ax_opco_chart_of_accts as(
    select *,
    rank() over(partition by opco_chart_of_accts_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_chart_of_accts
),
final as(
    select 
    *
    from v_ax_opco_chart_of_accts
    where rnk = 1 and delete_dtm is null
)

select * from final
),ax_opco_cust_open_trans as (
    select 
    current_timestamp as crt_dtm,
    cto._fivetran_synced as stg_load_dtm,
    case 
        when cto._fivetran_deleted = true then cto._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    cto.dataareaid as opco_id,
    cto.recid as src_key_txt,
    oct.opco_cust_trans_sk,
    opco.opco_sk,
    oc.opco_cust_sk,
    oca.opco_chart_of_accts_sk as cash_dscnt_gl_acct_sk,
    cto.transdate as trans_dt,
    cto.duedate as due_dt,
    cto.lastinterestdate as ltst_interest_calc_dt,
    cto.cashdiscdate as cash_dscnt_dt,
    case
        when cto.usecashdisc = 0 then 'Normal'
        when cto.usecashdisc = 1 then 'Always'
        when cto.usecashdisc = 2 then 'Never'
    end as cash_dscnt_usage_txt,
    cto.amountcur as trans_currency_trans_amt,
    cto.amountmst as opco_currency_trans_amt,
    cto.possiblecashdisc as included_cash_dscnt_amt
    from FIVETRAN.AX_PRODREPLICATION1_DBO.CUSTTRANSOPEN cto 
    left join __dbt__cte__v_ax_opco_cust_trans_curr oct 
        on cto.dataareaid = oct.opco_id
        and cto.refrecid = oct.src_key_txt
        and oct.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_curr opco 
        on cto.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cust_curr oc 
        on cto.dataareaid = oc.opco_id
        and cto.accountnum = oc.cust_id
        and oc.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_chart_of_accts_curr oca 
        on cto.dataareaid = oca.opco_id
        and cto.cashdiscaccount = oca.gl_acct_nbr
        and oca.src_sys_nm = 'AX'
    where cto.dataareaid not in ('044', '045', '047', '999')
),
final as (
    select md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(src_key_txt as 
    varchar
), '') as 
    varchar
)) as opco_cust_open_trans_sk,
           md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(src_key_txt as 
    varchar
), '') || '-' || coalesce(cast(opco_cust_trans_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cust_sk as 
    varchar
), '') || '-' || coalesce(cast(cash_dscnt_gl_acct_sk as 
    varchar
), '') || '-' || coalesce(cast(trans_dt as 
    varchar
), '') || '-' || coalesce(cast(due_dt as 
    varchar
), '') || '-' || coalesce(cast(ltst_interest_calc_dt as 
    varchar
), '') || '-' || coalesce(cast(cash_dscnt_dt as 
    varchar
), '') || '-' || coalesce(cast(cash_dscnt_usage_txt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_trans_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_trans_amt as 
    varchar
), '') || '-' || coalesce(cast(included_cash_dscnt_amt as 
    varchar
), '') as 
    varchar
)) as opco_cust_open_trans_ck,
           concat_ws('|', src_sys_nm, opco_id, src_key_txt) as opco_cust_open_trans_ak,
           *
    from ax_opco_cust_open_trans 
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_open_trans ) 
    
