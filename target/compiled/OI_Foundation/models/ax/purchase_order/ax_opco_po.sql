

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
),  __dbt__cte__v_ax_opco_trans_status_curr as (
with v_ax_opco_trans_status as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_trans_status
)

select * from v_ax_opco_trans_status
),  __dbt__cte__v_ax_opco_po_type_curr as (
with v_ax_opco_po_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_po_type

)

select * from v_ax_opco_po_type
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
),  __dbt__cte__v_ax_opco_sls_ordr_curr as (
with v_ax_opco_sls_ordr as(
    select *,
    rank() over(partition by opco_sls_ordr_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_sls_ordr
),
final as(
    select 
    *
    from v_ax_opco_sls_ordr
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
),  __dbt__cte__v_ax_opco_po_doc_status_curr as (
with v_ax_opco_po_doc_status as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_po_doc_status
)

select * from v_ax_opco_po_doc_status
),  __dbt__cte__v_ax_warehouse_curr as (
with v_ax_warehouse as(
    select *,
    rank() over(partition by warehouse_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_warehouse
),
final as(
    select 
    *
    from v_ax_warehouse
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
),  __dbt__cte__v_ax_opco_locn_curr as (
with v_ax_opco_locn as(
    select *,
    rank() over(partition by opco_locn_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_locn
),
final as(
    select 
    *
    from v_ax_opco_locn
    where rnk = 1 and delete_dtm is null
)

select * from final
),ax_opco_po as (
    select 
    current_timestamp as crt_dtm,
    pt._fivetran_synced as stg_load_dtm,
    case 
        when pt._fivetran_deleted = true then pt._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    pt.dataareaid as opco_id,
    pt.purchid as po_id,
    pt.purchname as po_nm,
    opco.opco_sk,
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    oip.opco_purpose_sk,
    ol.opco_lob_sk,
    ov.opco_vendor_sk,
    ov2.opco_vendor_sk as invoice_vendor_sk,
    ocd.opco_cash_dscnt_terms_sk,
    oc.opco_currency_sk as trans_currency_sk,
    ots.opco_trans_status_sk,
    opo.opco_po_type_sk,
    opt.opco_pymnt_terms_sk,
    opm.opco_pymnt_mode_sk,
    oso.opco_sls_ordr_sk as intercompany_orig_sls_ordr_sk,
    ocu.opco_cust_sk as intercompany_orig_cust_sk,
    odm.opco_dlvry_mode_sk,
    odt.opco_dlvry_terms_sk,
    opds.opco_po_doc_status_sk,
    wr.warehouse_sk,
    op.opco_project_sk,
    oln.opco_locn_sk,
    pt.deliverydate as dlvry_dt,
    pt.createddatetime as po_crt_dtm,
    pt.requestedby_opi as po_reqstd_by_nm,
    upper(pt.vendgroup) as vendor_grp_cd,
    pt.deliveryname as dlvry_vendor_nm,
    pt.vendorref as vendor_ref_txt,
    pt.namealias_opi as alias_nm,
    pt.itembuyergroupid as item_buyer_grp_id,
    pt.purchplacer as purchased_by_txt,
    pt.taxable_opi as taxable_ind,
    pt.discpercent as dscnt_pct,
    pt.cashdiscpercent as cash_dscnt_pct
    from FIVETRAN.AX_PRODREPLICATION1_DBO.PURCHTABLE pt
    left join __dbt__cte__v_ax_opco_curr opco 
        on pt.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cost_center_curr occ 
        on upper(pt.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dept_curr od 
        on upper(pt.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_type_curr ot 
        on upper(pt.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_purpose_curr oip
        on upper(pt.dimension4_) = oip.src_purpose_cd
        and oip.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_lob_curr ol
        on upper(pt.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_vendor_curr ov 
        on upper(pt.orderaccount) = ov.vendor_id
        and ov.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_vendor_curr ov2
        on upper(pt.invoiceaccount) = ov2.vendor_id
        and ov2.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cash_dscnt_terms_curr ocd 
        on upper(pt.cashdisc) = ocd.src_cash_dscnt_terms_cd
        and ocd.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_currency_curr oc 
        on upper(pt.currencycode) = oc.src_currency_cd
        and oc.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_trans_status_curr ots 
        on pt.purchstatus = ots.src_trans_status_cd
        and ots.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_po_type_curr opo
        on pt.purchasetype = opo.src_po_type_cd
        and opo.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_pymnt_terms_curr opt 
        on upper(pt.payment) = opt.src_pymnt_terms_cd
        and opt.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_pymnt_mode_curr opm 
        on upper(pt.paymmode) = opm.src_pymnt_mode_cd
        and opm.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cust_curr ocu 
        on pt.dataareaid = ocu.opco_id
        and pt.intercompanyoriginalcustacco12 = ocu.cust_id
        and ocu.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_sls_ordr_curr oso 
        on pt.dataareaid = oso.opco_id
        and pt.intercompanyoriginalsalesid = oso.sls_ordr_id
        and oso.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dlvry_mode_curr odm 
        on upper(pt.dlvmode) = odm.src_dlvry_mode_cd
        and odm.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dlvry_terms_curr odt 
        on upper(pt.dlvterm) = odt.src_dlvry_terms_cd
        and odt.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_po_doc_status_curr opds
        on pt.documentstatus = opds.po_doc_status_cd
        and opds.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_warehouse_curr wr 
        on pt.dataareaid = wr.opco_id
        and upper(pt.inventlocationid) = wr.warehouse_id
        and wr.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_project_curr op
        on pt.dataareaid = op.opco_id
        and pt.projid = op.project_id
        and op.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_locn_curr oln 
        on upper(pt.deliverystreet) = oln.addr_ln_1_txt
        and upper(pt.deliverycity) = oln.city_nm
        and upper(pt.dlvstate) = oln.state_nm
        and upper(pt.dlvcountryregionid) = oln.country_nm
        and upper(pt.deliveryzipcode) = oln.zip_cd
        and oln.src_sys_nm = 'AX'
    where pt.dataareaid not in ('044', '045', '047', '999')
),
final as (
    select distinct 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(po_id as 
    varchar
), '') as 
    varchar
)) as opco_po_sk,
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(po_id as 
    varchar
), '') || '-' || coalesce(cast(po_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_sk as 
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
), '') || '-' || coalesce(cast(opco_vendor_sk as 
    varchar
), '') || '-' || coalesce(cast(invoice_vendor_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cash_dscnt_terms_sk as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_trans_status_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_po_type_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_pymnt_terms_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_pymnt_mode_sk as 
    varchar
), '') || '-' || coalesce(cast(intercompany_orig_sls_ordr_sk as 
    varchar
), '') || '-' || coalesce(cast(intercompany_orig_cust_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_dlvry_mode_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_dlvry_terms_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_po_doc_status_sk as 
    varchar
), '') || '-' || coalesce(cast(warehouse_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_project_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_locn_sk as 
    varchar
), '') || '-' || coalesce(cast(dlvry_dt as 
    varchar
), '') || '-' || coalesce(cast(po_crt_dtm as 
    varchar
), '') || '-' || coalesce(cast(po_reqstd_by_nm as 
    varchar
), '') || '-' || coalesce(cast(vendor_grp_cd as 
    varchar
), '') || '-' || coalesce(cast(dlvry_vendor_nm as 
    varchar
), '') || '-' || coalesce(cast(vendor_ref_txt as 
    varchar
), '') || '-' || coalesce(cast(alias_nm as 
    varchar
), '') || '-' || coalesce(cast(item_buyer_grp_id as 
    varchar
), '') || '-' || coalesce(cast(purchased_by_txt as 
    varchar
), '') || '-' || coalesce(cast(taxable_ind as 
    varchar
), '') || '-' || coalesce(cast(dscnt_pct as 
    varchar
), '') || '-' || coalesce(cast(cash_dscnt_pct as 
    varchar
), '') as 
    varchar
)) as opco_po_ck,
            concat_ws('|', src_sys_nm, opco_id, po_id) as opco_po_ak,
            * 
    from ax_opco_po 
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_po ) 
    
