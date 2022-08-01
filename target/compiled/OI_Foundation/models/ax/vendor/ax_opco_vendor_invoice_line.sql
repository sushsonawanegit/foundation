

with  __dbt__cte__v_ax_opco_vendor_invoice_curr as (
with v_ax_opco_vendor_invoice as(
    select *,
    rank() over(partition by opco_vendor_invoice_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_vendor_invoice
),
final as(
    select 
    *
    from v_ax_opco_vendor_invoice
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
),  __dbt__cte__v_ax_opco_item_curr as (
with v_ax_opco_item as(
    select *,
    rank() over(partition by opco_item_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item
),
final as(
    select 
    *
    from v_ax_opco_item
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
),  __dbt__cte__v_ax_opco_assctn_curr as (
with v_ax_opco_assctn as(
    select *,
    rank() over(partition by opco_assctn_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_assctn
),
final as(
    select 
    *
    from v_ax_opco_assctn
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_invtry_ref_type_curr as (
with v_ax_opco_invtry_ref_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_invtry_ref_type

)

select * from v_ax_opco_invtry_ref_type
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
),ax_opco_vendor_invoice_line as (
    select 
    current_timestamp as crt_dtm,
    vit._fivetran_synced as stg_load_dtm,
    case
        when vit._fivetran_deleted = true then vit._fivetran_synced
        else null 
    end as delete_dtm,
    'AX' as src_sys_nm,
    vit.dataareaid as opco_id,
    vit.invoiceid as invoice_nbr,
    vit.internalinvoiceid as internal_invoice_nbr,
    vit.inventtransid as invtry_trans_id,
    ovi.opco_vendor_invoice_sk,
    opco.opco_sk,
    oi.opco_item_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    op.opco_purpose_sk,
    ol.opco_lob_sk,
    opo.opco_po_sk,
    opo1.opco_po_sk as orig_po_sk,
    ocoa.opco_chart_of_accts_sk as gl_acct_sk,
    ocr.opco_currency_sk as trans_currency_sk,
    oa.opco_assctn_sk,
    oirt.opco_invtry_ref_type_sk,
    ou.opco_uom_sk,
    vit.linenum as line_nbr,
    vit.invoicedate as invoice_dt,
    vit.name as item_desc,
    vit.externalitemid as vendor_item_id,
    vit.inventrefid as invtry_ref_id,
    vit.inventreftransid as invtry_ref_trans_id,
    vit.inventdate as invtry_trans_dt,
    vit.destcountryregionid as dest_country_nm,
    vit.deststate as dest_state_nm,
    vit.destcounty as dest_county_nm,
    vit.taxgroup as tax_grp_id,
    vit.taxitemgroup as tax_item_grp_id,
    vit.priceunit as per_unit_qty,
    vit.qty as purchase_qty,
    vit.qtyphysical as received_qty,
    vit.inventqty as invtry_qty,
    vit.discpercent as dscnt_pct,
    vit.linepercent as line_dscnt_pct,
    vit.purchprice as per_unit_price_amt,
    vit.lineamount as trans_currency_trans_amt,
    vit.lineamountmst as opco_currency_trans_amt,
    vit.purchmarkup as trans_currency_misc_chrgs_amt,
    vit.discamount as trans_currency_dscnt_amt,
    vit.linedisc as trans_currency_line_dscnt_amt,
    vit.multilndisc as trans_currency_multi_line_dscnt_amt,
    vit.lineamounttax as trans_currency_tax_amt,
    vit.tax1099amount as opco_currency_tax_1099_amt
    from FIVETRAN.AX_PRODREPLICATION1_DBO.VENDINVOICETRANS vit 
    left join __dbt__cte__v_ax_opco_vendor_invoice_curr ovi 
        on vit.dataareaid = ovi.opco_id
        and vit.invoiceid = ovi.invoice_nbr
        and vit.internalinvoiceid = ovi.internal_invoice_nbr
        and ovi.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_curr opco 
        on vit.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_item_curr oi 
        on vit.dataareaid = oi.opco_id
        and vit.itemid = oi.src_item_cd
        and oi.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cost_center_curr occ 
        on upper(vit.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dept_curr od 
        on upper(vit.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_type_curr ot 
        on upper(vit.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_purpose_curr op 
        on upper(vit.dimension4_) = op.src_purpose_cd
        and op.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_lob_curr ol 
        on upper(vit.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_po_curr opo 
        on vit.dataareaid = opo.opco_id 
        and vit.purchid = opo.po_id
        and opo.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_po_curr opo1
        on vit.dataareaid = opo1.opco_id 
        and vit.origpurchid = opo1.po_id
        and opo1.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_chart_of_accts_curr ocoa
        on vit.dataareaid = ocoa.opco_id
        and vit.ledgeraccount = ocoa.gl_acct_nbr
        and ocoa.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_currency_curr ocr 
        on upper(vit.currencycode) = ocr.src_currency_cd
        and ocr.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_assctn_curr oa
        on vit.dataareaid = oa.opco_id
        and vit.inventdimid = oa.src_assctn_cd
        and oa.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_invtry_ref_type_curr oirt 
        on vit.inventreftype = oirt.invtry_ref_type_cd
        and oirt.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_uom_curr ou 
        on upper(vit.purchunit) = ou.src_uom_cd
        and ou.src_sys_nm = 'AX'
    where vit.dataareaid not in ('044', '045', '047', '999')
),
final as(
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(invoice_nbr as 
    varchar
), '') || '-' || coalesce(cast(internal_invoice_nbr as 
    varchar
), '') || '-' || coalesce(cast(invtry_trans_id as 
    varchar
), '') as 
    varchar
)) as opco_vendor_invoice_line_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(invoice_nbr as 
    varchar
), '') || '-' || coalesce(cast(internal_invoice_nbr as 
    varchar
), '') || '-' || coalesce(cast(invtry_trans_id as 
    varchar
), '') || '-' || coalesce(cast(opco_vendor_invoice_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_item_sk as 
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
), '') || '-' || coalesce(cast(orig_po_sk as 
    varchar
), '') || '-' || coalesce(cast(gl_acct_sk as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_assctn_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_invtry_ref_type_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_uom_sk as 
    varchar
), '') || '-' || coalesce(cast(line_nbr as 
    varchar
), '') || '-' || coalesce(cast(invoice_dt as 
    varchar
), '') || '-' || coalesce(cast(item_desc as 
    varchar
), '') || '-' || coalesce(cast(vendor_item_id as 
    varchar
), '') || '-' || coalesce(cast(invtry_ref_id as 
    varchar
), '') || '-' || coalesce(cast(invtry_ref_trans_id as 
    varchar
), '') || '-' || coalesce(cast(invtry_trans_dt as 
    varchar
), '') || '-' || coalesce(cast(dest_country_nm as 
    varchar
), '') || '-' || coalesce(cast(dest_state_nm as 
    varchar
), '') || '-' || coalesce(cast(dest_county_nm as 
    varchar
), '') || '-' || coalesce(cast(tax_grp_id as 
    varchar
), '') || '-' || coalesce(cast(tax_item_grp_id as 
    varchar
), '') || '-' || coalesce(cast(per_unit_qty as 
    varchar
), '') || '-' || coalesce(cast(purchase_qty as 
    varchar
), '') || '-' || coalesce(cast(received_qty as 
    varchar
), '') || '-' || coalesce(cast(invtry_qty as 
    varchar
), '') || '-' || coalesce(cast(dscnt_pct as 
    varchar
), '') || '-' || coalesce(cast(line_dscnt_pct as 
    varchar
), '') || '-' || coalesce(cast(per_unit_price_amt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_trans_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_trans_amt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_misc_chrgs_amt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_dscnt_amt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_line_dscnt_amt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_multi_line_dscnt_amt as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_tax_amt as 
    varchar
), '') || '-' || coalesce(cast(opco_currency_tax_1099_amt as 
    varchar
), '') as 
    varchar
)) as opco_vendor_invoice_line_ck,
            concat_ws('|', src_sys_nm, opco_id, invoice_nbr, internal_invoice_nbr, invtry_trans_id) as opco_vendor_invoice_line_ak,
            * 
    from ax_opco_vendor_invoice_line 
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_vendor_invoice_line ) 
    
