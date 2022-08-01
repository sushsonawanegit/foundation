

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
),  __dbt__cte__v_ax_opco_vendor_curr as (
with v_ax_opco_vendor as(
    select *,
    rank() over(partition by opco_vendor_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_vendor
),
final as(
    select 
    *
    from v_ax_opco_vendor
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
),  __dbt__cte__v_ax_opco_po_curr as (
with v_ax_opco_po as(
    select *,
    rank() over(partition by opco_po_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_po
),
final as(
    select 
    *
    from v_ax_opco_po
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_gl_trans_type_curr as (
with v_ax_opco_gl_trans_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_gl_trans_type
)

select * from v_ax_opco_gl_trans_type
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
),  __dbt__cte__v_ax_opco_pymnt_mode_curr as (
with v_ax_opco_pymnt_mode as(
    select *,
    rank() over(partition by opco_pymnt_mode_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_pymnt_mode
),
final as(
    select 
    *
    from v_ax_opco_pymnt_mode
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_cash_dscnt_terms_curr as (
with v_ax_opco_cash_dscnt_terms as(
    select *,
    rank() over(partition by opco_cash_dscnt_terms_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cash_dscnt_terms
),
final as(
    select 
    *
    from v_ax_opco_cash_dscnt_terms
    where rnk = 1 and delete_dtm is null
)

select * from final
),ax_opco_vendor_trans as(
    select
    current_timestamp as crt_dtm,
    vt._fivetran_synced as stg_load_dtm,
    case
        when vt._fivetran_deleted = true then vt._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    vt.dataareaid as opco_id,
    vt.recid as src_key_txt,
    opco.opco_sk,
    ov.opco_vendor_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    op.opco_purpose_sk,
    ol.opco_lob_sk,
    opo.opco_po_sk,
    ogt.opco_gl_trans_type_sk,
    oc.opco_currency_sk as trans_currency_sk,
    opm.opco_pymnt_mode_sk,
    ocd.opco_cash_dscnt_terms_sk,
    opco1.opco_sk as ltst_settled_opco_sk,
    ov1.opco_vendor_sk as ltst_settled_vendor_sk,
    vt.txt as trans_desc,
    vt.transdate as trans_dt,
    vt.duedate as due_dt,
    vt.closed as trans_close_dt,
    vt.voucher as voucher_nbr,
    vt.invoice as invoice_nbr,
    vt.lastsettlevoucher as ltst_settled_voucher_nbr,
    vt.lastsettledate as ltst_settle_dt,
    vt.approved as trans_apprvd_ind,
    vt.approvedby as trans_apprvd_by_txt,
    vt.approveddate as trans_apprvd_dtm,
    vt.documentnum as document_nbr,
    vt.documentdate as document_dt,
    vt.offsetrecid as offset_src_key_txt,
    vt.invoiceproject as project_contract_orign_ind,
    vt.correct as crrctn_trans_ind,
    vt.settlement as auto_settle_ind,
    vt.cancel as cancelled_ind,
    vt.paymreference as pymnt_ref_txt,
    vt.paymid as pymnt_id,
    vt.amountcur as trans_currency_trans_amt,
    vt.settleamountcur as trans_currency_settled_amt,
    vt.amountmst as opco_currency_trans_amt,
    vt.settleamountmst as opco_currency_settled_amt,
    vt.exchrate as exchg_rt,
    vt.lastexchadj as ltst_exchg_adjstmt_dt,
    vt.exchadjustment as opco_currency_exchg_adjstmt_amt,
    vt.vendexchadjustmentrealized as opco_currency_realized_exchg_adjstmt_amt,
    vt.tax1099amount as opco_currency_tax_amt,
    vt.settletax1099amount as opco_currency_settled_tax_amt,
    vt.discdate_opi as dscnt_dt,
    vt.discamt_opi as dscnt_amt
    from FIVETRAN.AX_PRODREPLICATION1_DBO.VENDTRANS vt 
    left join __dbt__cte__v_ax_opco_curr opco1 
        on vt.lastsettlecompany = opco1.opco_id
        and opco1.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_curr opco
        on vt.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_vendor_curr ov 
        on upper(vt.accountnum) = ov.vendor_id
        and ov.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_vendor_curr ov1 
        on upper(vt.lastsettleaccountnum) = ov1.vendor_id
        and ov1.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cost_center_curr occ 
        on upper(vt.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dept_curr od 
        on upper(vt.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_type_curr ot 
        on upper(vt.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_purpose_curr op 
        on upper(vt.dimension4_) = op.src_purpose_cd
        and op.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_lob_curr ol 
        on upper(vt.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_po_curr opo 
        on vt.dataareaid = opo.opco_id 
        and vt.purchid_opi = opo.po_id
        and opo.src_sys_nm = 'AX'
        and vt.purchid_opi is not null
        and vt.purchid_opi <> '0'
    left join __dbt__cte__v_ax_opco_gl_trans_type_curr ogt 
        on cast(vt.transtype as string) = cast(ogt.src_gl_trans_type_cd as string)
        and ogt.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_currency_curr oc 
        on upper(vt.currencycode) = oc.src_currency_cd
        and oc.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_pymnt_mode_curr opm 
        on upper(vt.paymmode) = opm.src_pymnt_mode_cd
        and opm.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cash_dscnt_terms_curr ocd 
        on upper(vt.cashdisccode) = ocd.src_cash_dscnt_terms_cd
        and ocd.src_sys_nm = 'AX'
    where vt.dataareaid not in ('044', '045', '047', '999')
),
final as (
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(src_key_txt as 
    varchar
), '') as 
    varchar
)) as opco_vendor_trans_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(src_key_txt as 
    varchar
), '') || '-' || coalesce(cast(opco_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_vendor_sk as 
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
), '') || '-' || coalesce(cast(opco_po_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_gl_trans_type_sk as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_pymnt_mode_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cash_dscnt_terms_sk as 
    varchar
), '') || '-' || coalesce(cast(ltst_settled_opco_sk as 
    varchar
), '') || '-' || coalesce(cast(ltst_settled_vendor_sk as 
    varchar
), '') || '-' || coalesce(cast(trans_desc as 
    varchar
), '') || '-' || coalesce(cast(trans_dt as 
    varchar
), '') || '-' || coalesce(cast(due_dt as 
    varchar
), '') || '-' || coalesce(cast(trans_close_dt as 
    varchar
), '') || '-' || coalesce(cast(voucher_nbr as 
    varchar
), '') || '-' || coalesce(cast(invoice_nbr as 
    varchar
), '') || '-' || coalesce(cast(ltst_settled_voucher_nbr as 
    varchar
), '') || '-' || coalesce(cast(ltst_settle_dt as 
    varchar
), '') || '-' || coalesce(cast(trans_apprvd_ind as 
    varchar
), '') || '-' || coalesce(cast(trans_apprvd_by_txt as 
    varchar
), '') || '-' || coalesce(cast(trans_apprvd_dtm as 
    varchar
), '') || '-' || coalesce(cast(document_nbr as 
    varchar
), '') || '-' || coalesce(cast(document_dt as 
    varchar
), '') || '-' || coalesce(cast(offset_src_key_txt as 
    varchar
), '') || '-' || coalesce(cast(project_contract_orign_ind as 
    varchar
), '') || '-' || coalesce(cast(crrctn_trans_ind as 
    varchar
), '') || '-' || coalesce(cast(auto_settle_ind as 
    varchar
), '') || '-' || coalesce(cast(cancelled_ind as 
    varchar
), '') || '-' || coalesce(cast(pymnt_ref_txt as 
    varchar
), '') || '-' || coalesce(cast(pymnt_id as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_trans_amt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_settled_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_trans_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_settled_amt as 
    varchar
), '') || '-' || coalesce(cast(exchg_rt as 
    varchar
), '') || '-' || coalesce(cast(ltst_exchg_adjstmt_dt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_exchg_adjstmt_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_realized_exchg_adjstmt_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_tax_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_settled_tax_amt as 
    varchar
), '') || '-' || coalesce(cast(dscnt_dt as 
    varchar
), '') || '-' || coalesce(cast(dscnt_amt as 
    varchar
), '') as 
    varchar
)) as opco_vendor_trans_ck,
            concat_ws('|', src_sys_nm, opco_id, src_key_txt) as opco_vendor_trans_ak,
            * 
    from ax_opco_vendor_trans 
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_vendor_trans ) 
    
