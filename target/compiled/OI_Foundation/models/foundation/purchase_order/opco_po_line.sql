with opco_po_line as(
    select *,
    rank() over(partition by opco_po_line_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_po_line_lineage
),
final as(
    select 
    opco_po_line_sk, opco_po_line_ck ,opco_po_line_ak, CRT_DTM,  STG_LOAD_DTM, DELETE_DTM,
    src_sys_nm, opco_id, po_id, po_line_nbr, opco_po_sk, opco_vendor_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk,
    opco_assctn_sk, opco_item_sk, trans_currency_sk, opco_trans_status_sk, opco_po_type_sk, purchase_uom_sk, opco_gl_acct_sk, opco_invtry_ref_type_sk,
    opco_project_catgry_sk, opco_project_sk, invtry_trans_id, invtry_ref_id, invtry_ref_trans_id, project_item_trans_id, req_plan_schedule_id, req_plan_ordr_id,
    asset_id, project_tax_grp_id, po_line_nm, po_line_locked_ind, po_line_dlvry_complete_ind, scrap_ind,taxable_ind, tax_item_grp_cd, rqst_dlvry_dt,
    actl_dlvry_dt, dlvry_type_txt, po_line_crt_dtm, po_line_crt_by, vendor_grp_cd, vendor_item_id, invtry_unit_ordr_qty, purchase_unit_ordr_qty, 
    purchase_unit_financial_remaining_qty, purchase_unit_physical_remaining_qty, invtry_unit_physical_remaining_qty, invtry_unit_financial_remaining_qty,
    purchase_unit_received_qty, invtry_unit_received_qty, per_unit_qty, po_line_dscnt_pct, per_unit_line_dscnt_amt, per_unit_multi_line_dscnt_amt,
    unit_purchase_price_amt, po_line_amt, purchase_markup_amt, tax_1099_amt
    
    from opco_po_line
    where rnk = 1 and delete_dtm is null
)

select * from final