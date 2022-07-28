with opco_invtry_trans as(
    select *,
    rank() over(partition by opco_invtry_trans_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_invtry_trans_lineage') }}
),
final as(
    select 
    OPCO_INVTRY_TRANS_SK, OPCO_INVTRY_TRANS_CK, OPCO_INVTRY_TRANS_AK, CRT_DTM, STG_LOAD_DTM, DELETE_DTM,
	src_sys_nm, opco_id, src_key_txt, opco_sk, opco_cust_sk, opco_vendor_sk, opco_item_sk, opco_assctn_sk, trans_currency_sk,
    opco_invtry_trans_type_sk, opco_project_sk, opco_project_catgry_sk, opco_picking_list_sk, opco_invtry_issue_status_sk, 
    opco_invtry_rcpt_status_sk, invtry_trans_id, parent_invtry_trans_id, invtry_return_trans_id, invtry_transfer_trans_id, 
    ref_invtry_trans_id, trans_ref_id, item_route_id, item_bom_id, asset_id, packing_slip_id, project_adjstmt_ref_id, trans_dt,
    trans_status_dt, financial_trans_dt, financial_close_dt, rqst_ship_dt, cnfrm_ship_dt, item_expctd_movement_dt, invtry_qty_registered_dt,
    invtry_qty_picked_dt, invoice_nbr, voucher_nbr, invtry_updt_voucher_nbr, packing_slip_returned_item_ind, invoice_returned_item_ind,
    intercompany_transfer_ind, invtry_order_prcs_nbr, invtry_order_prcs_type_txt, trans_open_txt, invtry_trans_direction_txt, trans_qty, 
    settled_qty, posted_cost_amt, adjstmt_cost_amt, operations_cost_amt, settled_cost_amt, std_cost_amt, invtry_updt_qty_cost_amt, 
    invtry_updt_qty_revenue_amt
    from opco_invtry_trans
    where rnk = 1 and delete_dtm is null
)

select * from final