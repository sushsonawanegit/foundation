



with  __dbt__cte__v_ns_opco_curr as (
with v_ns_opco as(
    select *,
    rank() over(partition by opco_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco
),
final as(
    select 
    *
    from v_ns_opco
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ns_opco_chart_of_accts_curr as (
with v_ns_opco_chart_of_accts as(
    select *,
    rank() over(partition by opco_chart_of_accts_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_chart_of_accts
),
final as(
    select 
    *
    from v_ns_opco_chart_of_accts
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ns_opco_gl_trans_type_curr as (
with v_ns_opco_gl_trans_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_gl_trans_type
)

select * from v_ns_opco_gl_trans_type
),  __dbt__cte__v_ns_opco_cost_center_curr as (
with v_ns_opco_cost_center as(
    select *,
    rank() over(partition by opco_cost_center_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_cost_center
),
final as(
    select 
    *
    from v_ns_opco_cost_center
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ns_opco_dept_curr as (
with v_ns_opco_dept as(
    select *,
    rank() over(partition by opco_dept_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_dept
),
final as(
    select 
    *
    from v_ns_opco_dept
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ns_opco_type_curr as (
with v_ns_opco_type as(
    select *,
    rank() over(partition by opco_type_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_type
),
final as(
    select 
    *
    from v_ns_opco_type
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ns_opco_purpose_curr as (
with v_ns_opco_purpose as(
    select *,
    rank() over(partition by opco_purpose_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_purpose
),
final as(
    select 
    *
    from v_ns_opco_purpose
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ns_opco_currency_curr as (
with v_ns_opco_currency as(
    select *,
    rank() over(partition by opco_currency_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_currency
),
final as(
    select 
    *
    from v_ns_opco_currency
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ns_opco_uom_curr as (
with v_ns_opco_uom as(
    select *,
    rank() over(partition by opco_uom_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_uom
),
final as(
    select 
    *
    from v_ns_opco_uom
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ns_opco_brand_curr as (
with v_ns_opco_brand as(
    select *,
    rank() over(partition by opco_brand_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_brand
),
final as(
    select 
    *
    from v_ns_opco_brand
    where rnk = 1 and delete_dtm is null
)

select * from final
),ns_opco_gl_trans as (
    select 
    current_timestamp as crt_dtm,
    tl._fivetran_synced as stg_load_dtm,
    case 
        when tl._fivetran_deleted = true then tl._fivetran_synced
        else null
    end as delete_dtm,
    'NS'::varchar(20) as src_sys_nm,
    concat_ws('|', tl.transaction_id, tl.transaction_line_id)::varchar(50) as src_key_txt,
    t.trandate::timestamp as trans_dt,
    opco.opco_sk,
    oca.opco_chart_of_accts_sk,
    ogt.opco_gl_trans_type_sk,
    null::varchar(32) as opco_gl_trans_posting_type_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    oip.opco_purpose_sk,
    null::varchar(32) as opco_lob_sk,
    oc.opco_currency_sk as trans_currency_sk,
    null::varchar(32) as opco_proj_sk,
    null::varchar(32) as original_gl_opco_sk,
    null::varchar(20) as original_gl_voucher_nbr,
    t.document_number::varchar(20) as voucher_nbr,
    t.document_number::varchar(20) as journal_batch_nbr,
    tl.memo::varchar(5000) as gl_desc,
    t.document_number::varchar(20) as document_nbr,
    t.document_date::date as document_dt,
    null::numeric(1,0) as crrctn_trans_ind,
    case 
        when is_leftside = 'F' then 1
        when is_leftside = 'T' then 0
    end::numeric(1,0) as credit_ind,
    tl.opi_qty::float as gl_qty,
    tl.amount_foreign::float as trans_currency_gl_amt,
    tl.amount::float as opco_currency_gl_amt,
    fc.fscl_yr_id as fscl_yr_nbr,
    fc.fscl_mnth_nbr as fscl_mnth_nbr,
    case    
        when a.type_name <> 'Statistical' and  tl.non_posting_line = 'No' then 1
        when a.type_name <> 'Statistical' and  tl.non_posting_line = 'Yes' then 0
        when a.type_name = 'Statistical' then 1
    end::numeric(1,0) as posted_ind,
    ou.opco_uom_sk,
    ob.opco_brand_sk,
    null::varchar(32) as opco_sub_ledger_type_sk,
    null::varchar(32) as sub_ledger_cd
    from FIVETRAN.NETSUITE.TRANSACTION_LINES tl
    left join FIVETRAN.NETSUITE.TRANSACTIONS t 
        on tl.transaction_id = t.transaction_id
        and t._fivetran_deleted = false
    left join FIVETRAN.NETSUITE.ACCOUNTING_PERIODS ap 
        on t.accounting_period_id = ap.accounting_period_id
        and ap._fivetran_deleted = false
    left join FIVETRAN.NETSUITE.ACCOUNTS a
        on tl.account_id = a.account_id
        and a._fivetran_deleted = false
    left join __dbt__cte__v_ns_opco_curr opco 
        on tl.subsidiary_id = opco.opco_id
        and opco.src_sys_nm = 'NS'
    left join __dbt__cte__v_ns_opco_chart_of_accts_curr oca 
        on a.accountnumber = oca.gl_acct_nbr
        and tl.subsidiary_id = oca.opco_id
        and oca.src_sys_nm = 'NS'
    left join __dbt__cte__v_ns_opco_gl_trans_type_curr ogt 
        on upper(t.transaction_type) = ogt.src_gl_trans_type_cd
        and ogt.src_sys_nm = 'NS'
    left join __dbt__cte__v_ns_opco_cost_center_curr occ 
        on upper(tl.location_id) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'NS'
    left join __dbt__cte__v_ns_opco_dept_curr od 
        on upper(tl.department_id) = od.src_dept_cd
        and od.src_sys_nm = 'NS'
    left join __dbt__cte__v_ns_opco_type_curr ot 
        on upper(substr(a.opi_ax_acct, 8)) = ot.src_type_cd
        and ot.src_sys_nm = 'NS'
    left join __dbt__cte__v_ns_opco_purpose_curr oip
        on upper(tl.market_opi_id) = oip.src_purpose_cd
        and oip.src_sys_nm = 'NS'
    left join __dbt__cte__v_ns_opco_currency_curr oc
        on upper(t.currency_id) = oc.src_currency_cd
        and oc.src_sys_nm = 'NS'
    left join __dbt__cte__v_ns_opco_uom_curr ou 
        on ou.src_uom_cd = upper(a.opi_qty_uom_id)
        and ou.src_sys_nm = 'NS'
    left join __dbt__cte__v_ns_opco_brand_curr ob 
        on tl.class_id = ob.src_brand_cd
        and ob.src_sys_nm = 'NS'
    left join OI_DATA_DEV_V2.FND_DEV_BKP.fscl_clndr fc 
        on fc.clndr_dt = date(ap.ending)
    where tl.account_id is not null
),
final as (
    select  distinct
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_key_txt as 
    varchar
), '') as 
    varchar
)) as opco_gl_trans_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_key_txt as 
    varchar
), '') || '-' || coalesce(cast(trans_dt as 
    varchar
), '') || '-' || coalesce(cast(opco_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_chart_of_accts_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_gl_trans_type_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_gl_trans_posting_type_sk as 
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
), '') || '-' || coalesce(cast(trans_currency_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_proj_sk as 
    varchar
), '') || '-' || coalesce(cast(original_gl_opco_sk as 
    varchar
), '') || '-' || coalesce(cast(original_gl_voucher_nbr as 
    varchar
), '') || '-' || coalesce(cast(voucher_nbr as 
    varchar
), '') || '-' || coalesce(cast(journal_batch_nbr as 
    varchar
), '') || '-' || coalesce(cast(gl_desc as 
    varchar
), '') || '-' || coalesce(cast(document_nbr as 
    varchar
), '') || '-' || coalesce(cast(document_dt as 
    varchar
), '') || '-' || coalesce(cast(crrctn_trans_ind as 
    varchar
), '') || '-' || coalesce(cast(gl_qty as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_gl_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_gl_amt as 
    varchar
), '') || '-' || coalesce(cast(fscl_yr_nbr as 
    varchar
), '') || '-' || coalesce(cast(fscl_mnth_nbr as 
    varchar
), '') || '-' || coalesce(cast(posted_ind as 
    varchar
), '') || '-' || coalesce(cast(opco_uom_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_brand_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_sub_ledger_type_sk as 
    varchar
), '') || '-' || coalesce(cast(sub_ledger_cd as 
    varchar
), '') as 
    varchar
)) as opco_gl_trans_ck,
            concat_ws('|', src_sys_nm, src_key_txt)::varchar(100) as opco_gl_trans_ak,
            * 
    from ns_opco_gl_trans ogt
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_gl_trans ) 
    
