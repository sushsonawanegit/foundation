

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
),  __dbt__cte__v_ax_opco_invtry_trans_type_curr as (
with v_ax_opco_invtry_trans_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_invtry_trans_type
)

select * from v_ax_opco_invtry_trans_type
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
),  __dbt__cte__v_ax_opco_picking_list_curr as (
with v_ax_opco_picking_list as(
    select *,
    rank() over(partition by opco_picking_list_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_picking_list
),
final as(
    select 
    *
    from v_ax_opco_picking_list
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_invtry_issue_status_curr as (
with v_ax_opco_invtry_issue_status as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_invtry_issue_status
)

select * from v_ax_opco_invtry_issue_status
),  __dbt__cte__v_ax_opco_invtry_rcpt_status_curr as (
with v_ax_opco_invtry_rcpt_status as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_invtry_rcpt_status
)

select * from v_ax_opco_invtry_rcpt_status
),ax_opco_invtry_trans as(
    select 
    current_timestamp as crt_dtm,
    it._fivetran_synced as stg_load_dtm,
    case 
        when it._fivetran_deleted = true then it._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    it.dataareaid as opco_id,
    it.recid as src_key_txt,
    opco.opco_sk,
    oc.opco_cust_sk,
    ov.opco_vendor_sk,
    oi.opco_item_sk,
    oa.opco_assctn_sk,
    ocu.opco_currency_sk as trans_currency_sk,
    oitt.opco_invtry_trans_type_sk,
    op.opco_project_sk,
    opc.opco_project_catgry_sk,
    opl.opco_picking_list_sk,
    oiis.opco_invtry_issue_status_sk,
    oirc.opco_invtry_rcpt_status_sk,
    it.inventtransid as invtry_trans_id,
    it.inventtransidfather as parent_invtry_trans_id,
    it.inventtransidreturn as invtry_return_trans_id,
    it.inventtransidtransfer as invtry_transfer_trans_id,
    it.inventreftransid as ref_invtry_trans_id,
    it.transrefid as trans_ref_id,
    it.itemrouteid as item_route_id,
    it.itembomid as item_bom_id,
    it.assetid as asset_id,
    it.packingslipid as packing_slip_id,
    it.projadjustrefid as project_adjstmt_ref_id,
    it.datephysical as trans_dt,
    it.datestatus as trans_status_dt,
    it.datefinancial as financial_trans_dt,
    it.dateclosed as financial_close_dt,
    it.shippingdaterequested as rqst_ship_dt,
    it.shippingdateconfirmed as cnfrm_ship_dt,
    it.dateexpected as item_expctd_movement_dt,
    case 
        when it.direction in (0,1) then it.dateinvent
    end as invtry_qty_registered_dt,
    case 
        when it.direction in (0,2) then it.dateinvent
    end as invtry_qty_picked_dt,
    it.invoiceid as invoice_nbr,
    it.voucher as voucher_nbr,
    it.voucherphysical as invtry_updt_voucher_nbr,
    it.packingslipreturned as packing_slip_returned_item_ind,
    it.invoicereturned as invoice_returned_item_ind,
    it.intercompanytransfer_opi as intercompany_transfer_ind,
    it.transchildrefid as invtry_order_prcs_nbr,
    case 
        when it.transchildtype = 0 then NULL
        when it.transchildtype = 1 then 'Output Order'
        when it.transchildtype = 2 then 'Prod Picking List Journal'
        when it.transchildtype = 3 then 'Prod Reported As Finished Journal'
        when it.transchildtype = 4 then 'Inventory - Picking List Journal'
    end as invtry_order_prcs_type_txt,
    case
        when it.valueopen = 0 then 'No'
        when it.valueopen = 1 then 'Yes'
        when it.valueopen = 2 then 'Quotation'
    end as trans_open_txt,
    case
        when it.direction = 0 then NULL
        when it.direction = 1 then 'Receipt'
        when it.direction = 2 then 'Issue'
    end as invtry_trans_direction_txt,
    it.qty as trans_qty,
    it.qtysettled as settled_qty,
    it.costamountposted as posted_cost_amt,
    it.costamountadjustment as adjstmt_cost_amt,
    it.costamountoperations as operations_cost_amt,
    it.costamountsettled as settled_cost_amt,
    it.costamountstd as std_cost_amt,
    it.costamountphysical as invtry_updt_qty_cost_amt,
    it.revenueamountphysical as invtry_updt_qty_revenue_amt
    from FIVETRAN.AX_PRODREPLICATION1_DBO.INVENTTRANS it 
    left join __dbt__cte__v_ax_opco_curr opco 
        on it.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cust_curr oc
        on it.dataareaid = oc.opco_id
        and it.custvendac = oc.cust_id
        and it.direction = 2
        and it.custvendac <> ''
        and oc.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_vendor_curr ov
        on upper(it.custvendac) = ov.vendor_id
        and it.direction = 1
        and it.custvendac <> ''
        and ov.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_item_curr oi 
        on it.dataareaid = oi.opco_id
        and it.itemid = oi.src_item_cd
        and oi.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_assctn_curr oa 
        on it.dataareaid = oa.opco_id
        and it.inventdimid = oa.src_assctn_cd
        and oa.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_currency_curr ocu
        on upper(it.currencycode) = ocu.src_currency_cd
        and ocu.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_invtry_trans_type_curr oitt 
        on it.transtype = oitt.invtry_trans_type_cd
        and oitt.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_project_curr op 
        on it.dataareaid = op.opco_id
        and it.projid = op.project_id
        and op.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_project_catgry_curr opc 
        on it.dataareaid = opc.opco_id
        and it.projcategoryid = opc.catgry_cd
        and opc.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_picking_list_curr opl 
        on it.dataareaid = opl.opco_id
        and it.pickingrouteid = opl.picking_list_id
        and opl.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_invtry_issue_status_curr oiis 
        on it.statusissue = oiis.invtry_issue_status_cd
        and oiis.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_invtry_rcpt_status_curr oirc 
        on it.statusreceipt = oirc.invtry_rcpt_status_cd
        and oirc.src_sys_nm = 'AX'
    where it.dataareaid not in ('044', '045', '047', '999')
),
final as (
    select  distinct
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(src_key_txt as 
    varchar
), '') as 
    varchar
)) as opco_invtry_trans_sk,
			md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(src_key_txt as 
    varchar
), '') || '-' || coalesce(cast(opco_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cust_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_vendor_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_item_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_assctn_sk as 
    varchar
), '') || '-' || coalesce(cast(trans_currency_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_invtry_trans_type_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_project_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_project_catgry_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_picking_list_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_invtry_issue_status_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_invtry_rcpt_status_sk as 
    varchar
), '') || '-' || coalesce(cast(invtry_trans_id as 
    varchar
), '') || '-' || coalesce(cast(parent_invtry_trans_id as 
    varchar
), '') || '-' || coalesce(cast(invtry_return_trans_id as 
    varchar
), '') || '-' || coalesce(cast(invtry_transfer_trans_id as 
    varchar
), '') || '-' || coalesce(cast(ref_invtry_trans_id as 
    varchar
), '') || '-' || coalesce(cast(trans_ref_id as 
    varchar
), '') || '-' || coalesce(cast(item_route_id as 
    varchar
), '') || '-' || coalesce(cast(item_bom_id as 
    varchar
), '') || '-' || coalesce(cast(asset_id as 
    varchar
), '') || '-' || coalesce(cast(packing_slip_id as 
    varchar
), '') || '-' || coalesce(cast(project_adjstmt_ref_id as 
    varchar
), '') || '-' || coalesce(cast(trans_dt as 
    varchar
), '') || '-' || coalesce(cast(trans_status_dt as 
    varchar
), '') || '-' || coalesce(cast(financial_trans_dt as 
    varchar
), '') || '-' || coalesce(cast(financial_close_dt as 
    varchar
), '') || '-' || coalesce(cast(rqst_ship_dt as 
    varchar
), '') || '-' || coalesce(cast(cnfrm_ship_dt as 
    varchar
), '') || '-' || coalesce(cast(item_expctd_movement_dt as 
    varchar
), '') || '-' || coalesce(cast(invtry_qty_registered_dt as 
    varchar
), '') || '-' || coalesce(cast(invtry_qty_picked_dt as 
    varchar
), '') || '-' || coalesce(cast(invoice_nbr as 
    varchar
), '') || '-' || coalesce(cast(voucher_nbr as 
    varchar
), '') || '-' || coalesce(cast(invtry_updt_voucher_nbr as 
    varchar
), '') || '-' || coalesce(cast(packing_slip_returned_item_ind as 
    varchar
), '') || '-' || coalesce(cast(invoice_returned_item_ind as 
    varchar
), '') || '-' || coalesce(cast(intercompany_transfer_ind as 
    varchar
), '') || '-' || coalesce(cast(invtry_order_prcs_nbr as 
    varchar
), '') || '-' || coalesce(cast(invtry_order_prcs_type_txt as 
    varchar
), '') || '-' || coalesce(cast(trans_open_txt as 
    varchar
), '') || '-' || coalesce(cast(invtry_trans_direction_txt as 
    varchar
), '') || '-' || coalesce(cast(trans_qty as 
    varchar
), '') || '-' || coalesce(cast(settled_qty as 
    varchar
), '') || '-' || coalesce(cast(posted_cost_amt as 
    varchar
), '') || '-' || coalesce(cast(adjstmt_cost_amt as 
    varchar
), '') || '-' || coalesce(cast(operations_cost_amt as 
    varchar
), '') || '-' || coalesce(cast(settled_cost_amt as 
    varchar
), '') || '-' || coalesce(cast(std_cost_amt as 
    varchar
), '') || '-' || coalesce(cast(invtry_updt_qty_cost_amt as 
    varchar
), '') || '-' || coalesce(cast(invtry_updt_qty_revenue_amt as 
    varchar
), '') as 
    varchar
)) as opco_invtry_trans_ck,
			concat_ws('|', src_sys_nm, opco_id, src_key_txt) as opco_invtry_trans_ak, 
            * 		
    from ax_opco_invtry_trans 
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_invtry_trans ) 
    
