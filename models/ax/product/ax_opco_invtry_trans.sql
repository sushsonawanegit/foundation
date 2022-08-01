{% set _load = load_type('AX_OPCO_INVTRY_TRANS') %}

with ax_opco_invtry_trans as(
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
    from {{ source('AX_DEV', 'INVENTTRANS') }} it 
    left join {{ ref('ax_opco_curr')}} opco 
        on it.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_cust_curr')}} oc
        on it.dataareaid = oc.opco_id
        and it.custvendac = oc.cust_id
        and it.direction = 2
        and it.custvendac <> ''
        and oc.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_vendor_curr')}} ov
        on upper(it.custvendac) = ov.vendor_id
        and it.direction = 1
        and it.custvendac <> ''
        and ov.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_item_curr')}} oi 
        on it.dataareaid = oi.opco_id
        and it.itemid = oi.src_item_cd
        and oi.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_assctn_curr')}} oa 
        on it.dataareaid = oa.opco_id
        and it.inventdimid = oa.src_assctn_cd
        and oa.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_currency_curr')}} ocu
        on upper(it.currencycode) = ocu.src_currency_cd
        and ocu.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_invtry_trans_type_curr')}} oitt 
        on it.transtype = oitt.invtry_trans_type_cd
        and oitt.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_project_curr')}} op 
        on it.dataareaid = op.opco_id
        and it.projid = op.project_id
        and op.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_project_catgry_curr')}} opc 
        on it.dataareaid = opc.opco_id
        and it.projcategoryid = opc.catgry_cd
        and opc.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_picking_list_curr')}} opl 
        on it.dataareaid = opl.opco_id
        and it.pickingrouteid = opl.picking_list_id
        and opl.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_invtry_issue_status_curr')}} oiis 
        on it.statusissue = oiis.invtry_issue_status_cd
        and oiis.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_invtry_rcpt_status_curr')}} oirc 
        on it.statusreceipt = oirc.invtry_rcpt_status_cd
        and oirc.src_sys_nm = 'AX'
    where it.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (
    select  distinct
            {{ dbt_utils.surrogate_key(['src_sys_nm','opco_id','src_key_txt']) }} as opco_invtry_trans_sk,
			{{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'src_key_txt', 'opco_sk', 'opco_cust_sk', 'opco_vendor_sk', 'opco_item_sk', 'opco_assctn_sk', 'trans_currency_sk', 'opco_invtry_trans_type_sk', 'opco_project_sk', 'opco_project_catgry_sk', 'opco_picking_list_sk', 'opco_invtry_issue_status_sk', 'opco_invtry_rcpt_status_sk', 'invtry_trans_id', 'parent_invtry_trans_id', 'invtry_return_trans_id', 'invtry_transfer_trans_id', 'ref_invtry_trans_id', 'trans_ref_id', 'item_route_id', 'item_bom_id', 'asset_id', 'packing_slip_id', 'project_adjstmt_ref_id', 'trans_dt', 'trans_status_dt', 'financial_trans_dt', 'financial_close_dt', 'rqst_ship_dt', 'cnfrm_ship_dt', 'item_expctd_movement_dt', 'invtry_qty_registered_dt', 'invtry_qty_picked_dt', 'invoice_nbr', 'voucher_nbr', 'invtry_updt_voucher_nbr', 'packing_slip_returned_item_ind', 'invoice_returned_item_ind', 'intercompany_transfer_ind', 'invtry_order_prcs_nbr', 'invtry_order_prcs_type_txt', 'trans_open_txt', 'invtry_trans_direction_txt', 'trans_qty', 'settled_qty', 'posted_cost_amt', 'adjstmt_cost_amt', 'operations_cost_amt', 'settled_cost_amt', 'std_cost_amt', 'invtry_updt_qty_cost_amt', 'invtry_updt_qty_revenue_amt']) }} as opco_invtry_trans_ck,
			concat_ws('|', src_sys_nm, opco_id, src_key_txt) as opco_invtry_trans_ak, 
            * 		
    from ax_opco_invtry_trans 
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_INVTRY_TRANS') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_INVTRY_TRANS') and _load != 3%}
    union
    select
    OPCO_INVTRY_TRANS_SK, OPCO_INVTRY_TRANS_CK, OPCO_INVTRY_TRANS_AK, CRT_DTM, 
    (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
    (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
	src_sys_nm, opco_id, src_key_txt, opco_sk, opco_cust_sk, opco_vendor_sk, opco_item_sk, opco_assctn_sk, trans_currency_sk, opco_invtry_trans_type_sk, opco_project_sk, opco_project_catgry_sk, opco_picking_list_sk, opco_invtry_issue_status_sk, opco_invtry_rcpt_status_sk, invtry_trans_id, parent_invtry_trans_id, invtry_return_trans_id, invtry_transfer_trans_id, ref_invtry_trans_id, trans_ref_id, item_route_id, item_bom_id, asset_id, packing_slip_id, project_adjstmt_ref_id, trans_dt, trans_status_dt, financial_trans_dt, financial_close_dt, rqst_ship_dt, cnfrm_ship_dt, item_expctd_movement_dt, invtry_qty_registered_dt, invtry_qty_picked_dt, invoice_nbr, voucher_nbr, invtry_updt_voucher_nbr, packing_slip_returned_item_ind, invoice_returned_item_ind, intercompany_transfer_ind, invtry_order_prcs_nbr, invtry_order_prcs_type_txt, trans_open_txt, invtry_trans_direction_txt, trans_qty, settled_qty, posted_cost_amt, adjstmt_cost_amt, operations_cost_amt, settled_cost_amt, std_cost_amt, invtry_updt_qty_cost_amt, invtry_updt_qty_revenue_amt
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_INVTRY_TRANS
    {% if _load == 1 %}
        where OPCO_INVTRY_TRANS_ck not in (select distinct OPCO_INVTRY_TRANS_ck from final)
    {% else %}
        where OPCO_INVTRY_TRANS_ck not in (select distinct OPCO_INVTRY_TRANS_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_INVTRY_TRANS where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_INVTRY_TRANS_ck not in (select distinct OPCO_INVTRY_TRANS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_INVTRY_TRANS where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_INVTRY_TRANS_ck in (select distinct OPCO_INVTRY_TRANS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_INVTRY_TRANS where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}	