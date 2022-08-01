

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
),  __dbt__cte__v_ax_opco_gl_trans_type_curr as (
with v_ax_opco_gl_trans_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_gl_trans_type
)

select * from v_ax_opco_gl_trans_type
),  __dbt__cte__v_ax_opco_gl_trans_posting_type_curr as (
with v_ax_opco_gl_trans_posting_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_gl_trans_posting_type
)

select * from v_ax_opco_gl_trans_posting_type
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
),  __dbt__cte__v_ax_opco_currency_curr as (
with v_ax_opco_currency as(
    select *,
    rank() over(partition by opco_currency_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_currency
),
final as(
    select 
    *
    from v_ax_opco_currency
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_fscl_clndr_curr as (
with v_ax_fscl_clndr as(
    select *,
    rank() over(partition by fscl_clndr_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_fscl_clndr
),
final as(
    select 
    *
    from v_ax_fscl_clndr
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_uom_curr as (
with v_ax_opco_uom as(
    select *,
    rank() over(partition by opco_uom_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_uom
),
final as(
    select 
    *
    from v_ax_opco_uom
    where rnk = 1 and delete_dtm is null
)

select * from final
),ax_opco_gl_trans as (
    select 
    current_timestamp as crt_dtm,
    lt._fivetran_synced as stg_load_dtm,
    case 
        when lt._fivetran_deleted = true then lt._fivetran_synced
        else null
    end as delete_dtm,
    'AX'::varchar(20) as src_sys_nm,
    lt.recid::varchar(50) as src_key_txt,
    lt.transdate as trans_dt,
    opco.opco_sk,
    oca.opco_chart_of_accts_sk,
    ogt.opco_gl_trans_type_sk,
    ogtp.opco_gl_trans_posting_type_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    oip.opco_purpose_sk,
    ol.opco_lob_sk,
    oc.opco_currency_sk as trans_currency_sk,
    md5(cast(coalesce(cast('AX' as 
    varchar
), '') || '-' || coalesce(cast(lt.dataareaid as 
    varchar
), '') || '-' || coalesce(cast(lt.projid_opi as 
    varchar
), '') as 
    varchar
)) as opco_proj_sk,
    md5(cast(coalesce(cast('AX' as 
    varchar
), '') || '-' || coalesce(cast(lt.origincompany_opi as 
    varchar
), '') as 
    varchar
)) as original_gl_opco_sk,
    lt.originvoucher_opi::varchar(20) as original_gl_voucher_nbr,
    lt.voucher::varchar(20) as voucher_nbr,
    lt.journalnum::varchar(20) as journal_batch_nbr,
    lt.txt::varchar(5000) as gl_desc,
    lt.documentnum::varchar(20) as document_nbr,
    lt.documentdate::date as document_dt,
    lt.correct::numeric(1,0) as crrctn_trans_ind,
    lt.crediting::numeric(1,0) as credit_ind,
    case 
        when ltb.accountpltype = 100 and ltb.unitid_opi in ('$', null, ' ', '') then null
        when ltb.accountpltype = 100 and ltb.unitid_opi not in ('$', null, ' ', '') then lt.qty
        else lt.qty 
    end::float as gl_qty,
    lt.amountcur::float as trans_currency_gl_amt,
    case 
        when ltb.accountpltype = 100 and ltb.unitid_opi in ('$', null, ' ', '') then lt.qty
        when ltb.accountpltype = 100 and ltb.unitid_opi not in ('$', null, ' ', '') then null
        else lt.amountmst
    end::float as opco_currency_gl_amt,
    fc.fscl_yr_id as fscl_yr_nbr,
    fc.fscl_mnth_nbr as fscl_mnth_nbr,
    1::numeric(1,0) as posted_ind,
    ou.opco_uom_sk,
    null::varchar(32) as opco_brand_sk,
    null::varchar(32) as opco_sub_ledger_type_sk,
    null::varchar(32) as sub_ledger_cd
    from FIVETRAN.AX_PRODREPLICATION1_DBO.LEDGERTRANS lt
    left join FIVETRAN.AX_PRODREPLICATION1_DBO.LEDGERTABLE ltb 
        on lt.dataareaid = ltb.dataareaid
        and lt.accountnum = ltb.accountnum
        and ltb._fivetran_deleted = false
        and ltb.dataareaid not in ('044', '045', '047', '999')
    left join __dbt__cte__v_ax_opco_curr opco 
        on lt.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_chart_of_accts_curr oca 
        on lt.dataareaid = oca.opco_id
        and lt.accountnum = oca.gl_acct_nbr
        and oca.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_gl_trans_type_curr ogt 
        on lt.transtype::string = ogt.src_gl_trans_type_cd
        and ogt.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_gl_trans_posting_type_curr ogtp 
        on lt.posting = ogtp.src_gl_trans_posting_type_cd
        and ogtp.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cost_center_curr occ 
        on upper(lt.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dept_curr od 
        on upper(lt.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_type_curr ot 
        on upper(lt.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_purpose_curr oip
        on upper(lt.dimension4_) = oip.src_purpose_cd
        and oip.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_lob_curr ol
        on upper(lt.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_currency_curr oc
        on upper(lt.currencycode) = oc.src_currency_cd
        and oc.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_fscl_clndr_curr fc
        on fc.clndr_dt = date(lt.transdate)
    left join __dbt__cte__v_ax_opco_uom_curr ou 
        on ou.src_uom_cd = upper(ltb.unitid_opi)
        and ou.src_sys_nm = 'AX'
    where lt.periodcode = 1
    and lt.dataareaid not in ('044', '045', '047', '999')
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
), '') || '-' || coalesce(cast(credit_ind as 
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
    from ax_opco_gl_trans 
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_gl_trans ) 
    
