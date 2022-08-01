

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
),  __dbt__cte__v_ax_opco_po_type_curr as (
with v_ax_opco_po_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_po_type

)

select * from v_ax_opco_po_type
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
),  __dbt__cte__v_ax_opco_pymnt_terms_curr as (
with v_ax_opco_pymnt_terms as(
    select *,
    rank() over(partition by opco_pymnt_terms_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_pymnt_terms
),
final as(
    select 
    *
    from v_ax_opco_pymnt_terms
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
),  __dbt__cte__v_ax_opco_dlvry_terms_curr as (
with v_ax_opco_dlvry_terms as(
    select *,
    rank() over(partition by opco_dlvry_terms_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_dlvry_terms
),
final as(
    select 
    *
    from v_ax_opco_dlvry_terms
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_dlvry_mode_curr as (
with v_ax_opco_dlvry_mode as(
    select *,
    rank() over(partition by opco_dlvry_mode_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_dlvry_mode
),
final as(
    select 
    *
    from v_ax_opco_dlvry_mode
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_project_curr as (
with v_ax_opco_project as(
    select *,
    rank() over(partition by opco_project_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project
),
final as(
    select 
    *
    from v_ax_opco_project
    where rnk = 1 and delete_dtm is null
)

select * from final
),ax_opco_vendor_invoice as(
    select
    current_timestamp as crt_dtm,
    vij._fivetran_synced as stg_load_dtm,
    case
        when vij._fivetran_deleted = true then vij._fivetran_synced 
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    vij.dataareaid as opco_id,
    vij.invoiceid as invoice_nbr,
    vij.internalinvoiceid as internal_invoice_nbr,
    opco.opco_sk,
    opo.opco_po_sk,
    ov.opco_vendor_sk,
    ov1.opco_vendor_sk as opco_invoice_vendor_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    op.opco_purpose_sk,
    ol.opco_lob_sk,
    opt.opco_po_type_sk,
    oc.opco_currency_sk as trans_currency_sk,
    opm.opco_pymnt_terms_sk,
    ocd.opco_cash_dscnt_terms_sk,
    odt.opco_dlvry_terms_sk,
    odm.opco_dlvry_mode_sk,
    opr.opco_project_sk,
    vij.vendgroup as vendor_grp_cd,
    vij.invoicedate as invoice_dt,
    vij.duedate as due_dt,
    vij.ledgervoucher as gl_voucher_nbr,
    vij.documentnum as document_nbr,
    vij.documentdate as document_dt,
    vij.cashdiscdate as cash_dscnt_dt,
    vij.countryregionid as default_country_nm,
    vij.purchplacer_opi as purchaser_nm,
    vij.qty as invoice_qty,
    vij.weight as invoice_weight_amt,
    vij.volume as invoice_vol_amt,
    vij.requestedby_opi as rqstd_by_nm,
    vij.exchrate as exchg_rt,
    vij.paymid as pymnt_id,
    vij.taxgroup as tax_grp_id,
    vij.itembuyergroupid as item_buyer_grp_id,
    vij.fixedduedate as fixed_due_dt,
    vij.invoiceamount as trans_currency_invoice_amt,
    vij.invoiceamountmst as opco_currency_invoice_amt,
    vij.invoiceroundoff as trans_currency_invoice_roundoff_amt,
    vij.summarkup as trans_currency_misc_chrgs_amt,
    vij.summarkupmst as opco_currency_misc_chrgs_amt,
    vij.cashdisc as trans_currency_cash_dscnt_amt,
    vij.sumlinedisc as trans_currency_line_dscnt_amt,
    vij.salesbalance as trans_currency_balance_sls_amt    
    from FIVETRAN.AX_PRODREPLICATION1_DBO.VENDINVOICEJOUR vij
    left join __dbt__cte__v_ax_opco_curr opco 
        on vij.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_po_curr opo 
        on vij.dataareaid = opo.opco_id
        and vij.purchid = opo.po_id
        and opo.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_vendor_curr ov 
        on upper(vij.orderaccount) = ov.vendor_id
        and ov.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_vendor_curr ov1 
        on upper(vij.invoiceaccount) = ov1.vendor_id
        and ov1.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cost_center_curr occ 
        on upper(vij.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dept_curr od 
        on upper(vij.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_type_curr ot 
        on upper(vij.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_purpose_curr op 
        on upper(vij.dimension4_) = op.src_purpose_cd
        and op.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_lob_curr ol 
        on upper(vij.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_po_type_curr opt
        on vij.purchasetype = opt.src_po_type_cd
        and opt.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_currency_curr oc 
        on upper(vij.currencycode) = oc.src_currency_cd
        and oc.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_pymnt_terms_curr opm 
        on upper(vij.payment) = opm.src_pymnt_terms_cd
        and opm.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cash_dscnt_terms_curr ocd 
        on upper(vij.cashdisccode) = ocd.src_cash_dscnt_terms_cd
        and ocd.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dlvry_terms_curr odt 
        on upper(vij.dlvterm) = odt.src_dlvry_terms_cd
        and odt.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dlvry_mode_curr odm 
        on upper(vij.dlvmode) = odm.src_dlvry_mode_cd
        and odm.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_project_curr opr 
        on vij.dataareaid = opr.opco_id
        and vij.projid_opi = opr.project_id
        and opr.src_sys_nm = 'AX'
    where vij.dataareaid not in ('044', '045', '047', '999')
),
final as (
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(invoice_nbr as 
    varchar
), '') || '-' || coalesce(cast(internal_invoice_nbr as 
    varchar
), '') as 
    varchar
)) as opco_vendor_invoice_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(invoice_nbr as 
    varchar
), '') || '-' || coalesce(cast(internal_invoice_nbr as 
    varchar
), '') || '-' || coalesce(cast(opco_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_po_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_vendor_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_invoice_vendor_sk as 
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
), '') || '-' || coalesce(cast(opco_po_type_sk as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_pymnt_terms_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cash_dscnt_terms_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_dlvry_terms_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_dlvry_mode_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_project_sk as 
    varchar
), '') || '-' || coalesce(cast(vendor_grp_cd as 
    varchar
), '') || '-' || coalesce(cast(invoice_dt as 
    varchar
), '') || '-' || coalesce(cast(due_dt as 
    varchar
), '') || '-' || coalesce(cast(gl_voucher_nbr as 
    varchar
), '') || '-' || coalesce(cast(document_nbr as 
    varchar
), '') || '-' || coalesce(cast(document_dt as 
    varchar
), '') || '-' || coalesce(cast(cash_dscnt_dt as 
    varchar
), '') || '-' || coalesce(cast(default_country_nm as 
    varchar
), '') || '-' || coalesce(cast(purchaser_nm as 
    varchar
), '') || '-' || coalesce(cast(invoice_qty as 
    varchar
), '') || '-' || coalesce(cast(invoice_weight_amt as 
    varchar
), '') || '-' || coalesce(cast(invoice_vol_amt as 
    varchar
), '') || '-' || coalesce(cast(rqstd_by_nm as 
    varchar
), '') || '-' || coalesce(cast(exchg_rt as 
    varchar
), '') || '-' || coalesce(cast(pymnt_id as 
    varchar
), '') || '-' || coalesce(cast(tax_grp_id as 
    varchar
), '') || '-' || coalesce(cast(item_buyer_grp_id as 
    varchar
), '') || '-' || coalesce(cast(fixed_due_dt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_invoice_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_invoice_amt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_invoice_roundoff_amt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_misc_chrgs_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_misc_chrgs_amt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_cash_dscnt_amt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_line_dscnt_amt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_balance_sls_amt as 
    varchar
), '') as 
    varchar
)) as opco_vendor_invoice_ck,
            concat_ws('|', src_sys_nm, opco_id, invoice_nbr, internal_invoice_nbr) as opco_vendor_invoice_ak,
            * 
    from ax_opco_vendor_invoice 
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_vendor_invoice ) 
    
