with opco_vendor_packing_slip as(
    select *,
    rank() over(partition by opco_vendor_packing_slip_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_vendor_packing_slip_lineage
),
final as(
    select 
    opco_vendor_packing_slip_sk, opco_vendor_packing_slip_ck,opco_vendor_packing_slip_ak, crt_dtm, 
    STG_LOAD_DTM, DELETE_DTM,
    src_sys_nm, opco_id, packing_slip_id, internal_packing_slip_id, opco_sk, opco_vendor_sk, opco_invoice_vendor_sk, opco_po_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_dlvry_terms_sk, opco_dlvry_mode_sk, dlvry_locn_sk, opco_project_sk, opco_po_type_sk, dlvry_dt, dlvry_vendor_nm, gl_voucher_nbr, item_buyer_grp_id, purchaser_nm, rqstd_by_nm, requsitioner_nm, attn_info_txt, dlvry_qty, dlvry_volume_amt, dlvry_weight_amt


    from opco_vendor_packing_slip
    where rnk = 1 and delete_dtm is null
)

select * from final