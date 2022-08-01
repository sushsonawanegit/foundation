

with  __dbt__cte__v_ax_opco_po_curr as (
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
),  __dbt__cte__v_ax_opco_invtry_ref_type_curr as (
with v_ax_opco_invtry_ref_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_invtry_ref_type

)

select * from v_ax_opco_invtry_ref_type
),  __dbt__cte__v_ax_opco_project_catgry_curr as (
with v_ax_opco_project_catgry as(
    select *,
    rank() over(partition by opco_project_catgry_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_catgry
),
final as(
    select 
    *
    from v_ax_opco_project_catgry
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
),ax_opco_po_line as(
    select 
    current_timestamp as crt_dtm,
    pl._fivetran_synced as stg_load_dtm,
    case
        when pl._fivetran_deleted = true then pl._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    pl.dataareaid as opco_id,
    pl.purchid as po_id,
    pl.linenum as po_line_nbr,
    op.opco_po_sk,
    ov.opco_vendor_sk, 
    occ.opco_cost_center_sk,
    od.opco_dept_sk,
    ot.opco_type_sk,
    oip.opco_purpose_sk,
    ol.opco_lob_sk,
    oa.opco_assctn_sk,
    oi.opco_item_sk,
    oc.opco_currency_sk as trans_currency_sk,
    ots.opco_trans_status_sk,
    opt.opco_po_type_sk,
    ou.opco_uom_sk as purchase_uom_sk,
    ocoa.opco_chart_of_accts_sk as opco_gl_acct_sk,
    oirt.opco_invtry_ref_type_sk,
    opc.opco_project_catgry_sk,
    opr.opco_project_sk,
    pl.inventtransid as invtry_trans_id,
    pl.inventrefid as invtry_ref_id,
    pl.inventreftransid as invtry_ref_trans_id,
    pl.projtransid as project_item_trans_id,
    upper(pl.reqplanidsched) as req_plan_schedule_id,
    pl.reqpoid as req_plan_ordr_id,
    pl.assetid as asset_id,
    pl.projtaxgroupid as project_tax_grp_id,
    pl.name as po_line_nm,
    pl.blocked as po_line_locked_ind,
    pl.complete as po_line_dlvry_complete_ind,
    pl.scrap as scrap_ind,
    pl.taxable_opi as taxable_ind,
    pl.taxitemgroup as tax_item_grp_cd,
    pl.deliverydate as rqst_dlvry_dt,
    pl.confirmeddlv as actl_dlvry_dt,
    case    
        when pl.deliverytype = 0 then null
        when pl.deliverytype = 1 then 'Direct Delivery'
    end as dlvry_type_txt,
    pl.createddatetime as po_line_crt_dtm,
    pl.createdby as po_line_crt_by,
    upper(pl.vendgroup) as vendor_grp_cd,
    pl.externalitemid as vendor_item_id,
    pl.qtyordered as invtry_unit_ordr_qty,
    pl.purchqty as purchase_unit_ordr_qty,
    pl.remainpurchfinancial as purchase_unit_financial_remaining_qty,
    pl.remainpurchphysical as purchase_unit_physical_remaining_qty,
    pl.remaininventphysical as invtry_unit_physical_remaining_qty,
    pl.remaininventfinancial as invtry_unit_financial_remaining_qty,
    pl.purchreceivednow as purchase_unit_received_qty,
    pl.inventreceivednow as invtry_unit_received_qty,
    pl.priceunit as per_unit_qty,
    pl.linepercent as po_line_dscnt_pct,
    pl.linedisc as per_unit_line_dscnt_amt,
    pl.multilndisc as per_unit_multi_line_dscnt_amt,
    pl.purchprice as unit_purchase_price_amt,
    pl.lineamount as po_line_amt,
    pl.purchmarkup as purchase_markup_amt,
    pl.tax1099amount as tax_1099_amt
    from FIVETRAN.AX_PRODREPLICATION1_DBO.PURCHLINE pl 
    left join __dbt__cte__v_ax_opco_po_curr op 
        on pl.dataareaid = op.opco_id
        and pl.purchid = op.po_id
        and op.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_vendor_curr ov
        on upper(pl.vendaccount) = ov.vendor_id
        and ov.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cost_center_curr occ 
        on upper(pl.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dept_curr od 
        on upper(pl.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_type_curr ot 
        on upper(pl.dimension3_) = ot.src_type_cd
        and ot.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_purpose_curr oip
        on upper(pl.dimension4_) = oip.src_purpose_cd
        and oip.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_lob_curr ol
        on upper(pl.dimension5_) = ol.src_lob_cd
        and ol.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_assctn_curr oa
        on pl.dataareaid = oa.opco_id
        and pl.inventdimid = oa.src_assctn_cd
        and oa.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_item_curr oi 
        on pl.dataareaid = oi.opco_id
        and pl.itemid = oi.src_item_cd
        and oi.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_currency_curr oc 
        on upper(pl.currencycode) = oc.src_currency_cd
        and oc.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_trans_status_curr ots 
        on pl.purchstatus = ots.src_trans_status_cd
        and ots.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_po_type_curr opt 
        on pl.purchasetype = opt.src_po_type_cd
        and opt.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_uom_curr ou 
        on upper(pl.priceunit) = ou.src_uom_cd
        and ou.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_chart_of_accts_curr ocoa
        on pl.dataareaid = ocoa.opco_id
        and pl.ledgeraccount = ocoa.gl_acct_nbr
        and ocoa.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_invtry_ref_type_curr oirt 
        on pl.itemreftype = oirt.invtry_ref_type_cd
        and oirt.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_project_catgry_curr opc
        on pl.dataareaid = opc.opco_id
        and pl.projcategoryid = opc.catgry_cd
        and opc.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_project_curr opr
        on pl.dataareaid = opr.opco_id
        and pl.projid = opr.project_id
        and opr.src_sys_nm = 'AX'
    where pl.dataareaid not in ('044', '045', '047', '999')
),
final as (
    select distinct 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(po_id as 
    varchar
), '') || '-' || coalesce(cast(po_line_nbr as 
    varchar
), '') || '-' || coalesce(cast(invtry_trans_id as 
    varchar
), '') as 
    varchar
)) as opco_po_line_sk,
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(po_id as 
    varchar
), '') || '-' || coalesce(cast(po_line_nbr as 
    varchar
), '') || '-' || coalesce(cast(opco_po_sk as 
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
), '') || '-' || coalesce(cast(opco_assctn_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_item_sk as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_trans_status_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_po_type_sk as 
    varchar
), '') || '-' || coalesce(cast(purchase_uom_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_gl_acct_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_invtry_ref_type_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_project_catgry_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_project_sk as 
    varchar
), '') || '-' || coalesce(cast(invtry_trans_id as 
    varchar
), '') || '-' || coalesce(cast(invtry_ref_id as 
    varchar
), '') || '-' || coalesce(cast(invtry_ref_trans_id as 
    varchar
), '') || '-' || coalesce(cast(project_item_trans_id as 
    varchar
), '') || '-' || coalesce(cast(req_plan_schedule_id as 
    varchar
), '') || '-' || coalesce(cast(req_plan_ordr_id as 
    varchar
), '') || '-' || coalesce(cast(asset_id as 
    varchar
), '') || '-' || coalesce(cast(project_tax_grp_id as 
    varchar
), '') || '-' || coalesce(cast(po_line_nm as 
    varchar
), '') || '-' || coalesce(cast(po_line_locked_ind as 
    varchar
), '') || '-' || coalesce(cast(po_line_dlvry_complete_ind as 
    varchar
), '') || '-' || coalesce(cast(scrap_ind as 
    varchar
), '') || '-' || coalesce(cast(taxable_ind as 
    varchar
), '') || '-' || coalesce(cast(tax_item_grp_cd as 
    varchar
), '') || '-' || coalesce(cast(rqst_dlvry_dt as 
    varchar
), '') || '-' || coalesce(cast(actl_dlvry_dt as 
    varchar
), '') || '-' || coalesce(cast(dlvry_type_txt as 
    varchar
), '') || '-' || coalesce(cast(po_line_crt_dtm as 
    varchar
), '') || '-' || coalesce(cast(po_line_crt_by as 
    varchar
), '') || '-' || coalesce(cast(vendor_grp_cd as 
    varchar
), '') || '-' || coalesce(cast(vendor_item_id as 
    varchar
), '') || '-' || coalesce(cast(invtry_unit_ordr_qty as 
    varchar
), '') || '-' || coalesce(cast(purchase_unit_ordr_qty as 
    varchar
), '') || '-' || coalesce(cast(purchase_unit_financial_remaining_qty as 
    varchar
), '') || '-' || coalesce(cast(purchase_unit_physical_remaining_qty as 
    varchar
), '') || '-' || coalesce(cast(invtry_unit_physical_remaining_qty as 
    varchar
), '') || '-' || coalesce(cast(invtry_unit_financial_remaining_qty as 
    varchar
), '') || '-' || coalesce(cast(purchase_unit_received_qty as 
    varchar
), '') || '-' || coalesce(cast(invtry_unit_received_qty as 
    varchar
), '') || '-' || coalesce(cast(per_unit_qty as 
    varchar
), '') || '-' || coalesce(cast(po_line_dscnt_pct as 
    varchar
), '') || '-' || coalesce(cast(per_unit_line_dscnt_amt as 
    varchar
), '') || '-' || coalesce(cast(per_unit_multi_line_dscnt_amt as 
    varchar
), '') || '-' || coalesce(cast(unit_purchase_price_amt as 
    varchar
), '') || '-' || coalesce(cast(po_line_amt as 
    varchar
), '') || '-' || coalesce(cast(purchase_markup_amt as 
    varchar
), '') || '-' || coalesce(cast(tax_1099_amt as 
    varchar
), '') as 
    varchar
)) as opco_po_line_ck,
            concat_ws('|', src_sys_nm, opco_id, po_id, po_line_nbr, invtry_trans_id) as opco_po_line_ak, 
            * 
    from ax_opco_po_line 
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_po_line ) 
    
