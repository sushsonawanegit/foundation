

with  __dbt__cte__v_ax_opco_curr as (
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
),  __dbt__cte__v_ax_opco_cust_trans_curr as (
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
),  __dbt__cte__v_ax_opco_cost_center_curr as (
with v_ax_opco_cost_center as(
    select *,
    rank() over(partition by opco_cost_center_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cost_center
),
final as(
    select 
    *
    from v_ax_opco_cost_center
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_dept_curr as (
with v_ax_opco_dept as(
    select *,
    rank() over(partition by opco_dept_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_dept
),
final as(
    select 
    *
    from v_ax_opco_dept
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_type_curr as (
with v_ax_opco_type as(
    select *,
    rank() over(partition by opco_type_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_type
),
final as(
    select 
    *
    from v_ax_opco_type
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_purpose_curr as (
with v_ax_opco_purpose as(
    select *,
    rank() over(partition by opco_purpose_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_purpose
),
final as(
    select 
    *
    from v_ax_opco_purpose
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_lob_curr as (
with v_ax_opco_lob as(
    select *,
    rank() over(partition by opco_lob_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_lob
),
final as(
    select 
    *
    from v_ax_opco_lob
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
),ax_opco_cust_settled_trans as (
    select
    current_timestamp as crt_dtm,
    cs._fivetran_synced as stg_load_dtm,
    case
        when cs._fivetran_deleted = true then cs._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    cs.dataareaid as opco_id,
    cs.recid as src_key_txt,
    opco.opco_sk,
    oct.opco_cust_trans_sk,
    oc.opco_cust_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    op.opco_purpose_sk,
    ol.opco_lob_sk,
    oca.opco_chart_of_accts_sk as cash_dscnt_gl_acct_sk,
    cs.transdate as trans_dt,
    cs.duedate as due_dt,
    cs.createddatetime as src_crt_dtm,
    oc1.opco_cust_sk as offset_cust_sk,
    oct1.opco_cust_trans_sk as offset_cust_trans_sk,
    cs.offsettransvoucher as offset_voucher_nbr,
    cs.custcashdiscdate as cust_cash_dscnt_dt,
    cs.lastinterestdate as latest_interest_calc_dt,
    cs.canbereversed as reversible_ind,
    cs.settlementgroup as settlement_grp_txt,
    cs.settlementvoucher as settled_voucher_nbr,
    cs.settleamountcur as trans_currency_settled_amt,
    cs.settleamountmst as opco_currency_settled_amt,
    cs.exchadjustment as exch_adjstmnt_amt,
    cs.utilizedcashdisc as utilized_cash_dscnt_amt
    from FIVETRAN.AX_PRODREPLICATION1_DBO.CUSTSETTLEMENT cs
    left join __dbt__cte__v_ax_opco_curr opco 
        on cs.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cust_trans_curr oct
        on cs.dataareaid = oct.opco_id
        and cs.transrecid = oct.src_key_txt
        and oct.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cust_curr oc
        on cs.dataareaid = oc.opco_id
        and cs.accountnum = oc.cust_id
        and oc.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cust_trans_curr oct1
        on cs.dataareaid = oct1.opco_id
        and cs.offsetrecid = oct1.src_key_txt
        and oct1.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cust_curr oc1
        on cs.offsetcompany = oc1.opco_id
        and cs.offsetaccountnum = oc1.cust_id
        and oc1.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cost_center_curr occ 
        on upper(cs.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dept_curr od 
        on upper(cs.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_type_curr ot 
        on upper(cs.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_purpose_curr op 
        on upper(cs.dimension4_) = op.src_purpose_cd
        and op.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_lob_curr ol 
        on upper(cs.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_chart_of_accts_curr oca 
        on cs.dataareaid = oca.opco_id
        and cs.cashdiscaccount = oca.gl_acct_nbr
        and oca.src_sys_nm = 'AX'
    where cs.dataareaid not in ('044', '045', '047', '999')
),
final as(
    select distinct
           md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(src_key_txt as 
    varchar
), '') as 
    varchar
)) as opco_cust_settled_trans_sk,
           md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(src_key_txt as 
    varchar
), '') || '-' || coalesce(cast(opco_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cust_trans_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cust_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cost_center_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_dept_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_type_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_purpose_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_lob_sk as 
    varchar
), '') || '-' || coalesce(cast(cash_dscnt_gl_acct_sk as 
    varchar
), '') || '-' || coalesce(cast(trans_dt as 
    varchar
), '') || '-' || coalesce(cast(due_dt as 
    varchar
), '') || '-' || coalesce(cast(src_crt_dtm as 
    varchar
), '') || '-' || coalesce(cast(offset_cust_sk as 
    varchar
), '') || '-' || coalesce(cast(offset_cust_trans_sk as 
    varchar
), '') || '-' || coalesce(cast(offset_voucher_nbr as 
    varchar
), '') || '-' || coalesce(cast(cust_cash_dscnt_dt as 
    varchar
), '') || '-' || coalesce(cast(latest_interest_calc_dt as 
    varchar
), '') || '-' || coalesce(cast(reversible_ind as 
    varchar
), '') || '-' || coalesce(cast(settlement_grp_txt as 
    varchar
), '') || '-' || coalesce(cast(settled_voucher_nbr as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_settled_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_settled_amt as 
    varchar
), '') || '-' || coalesce(cast(exch_adjstmnt_amt as 
    varchar
), '') || '-' || coalesce(cast(utilized_cash_dscnt_amt as 
    varchar
), '') as 
    varchar
)) as opco_cust_settled_trans_ck,
           concat_ws('|', src_sys_nm, opco_id, src_key_txt) as opco_cust_settled_trans_ak, *
    from ax_opco_cust_settled_trans
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_settled_trans ) 
    
