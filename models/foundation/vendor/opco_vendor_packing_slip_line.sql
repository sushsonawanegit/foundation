with opco_vendor_packing_slip_line as(
    select *,
    rank() over(partition by opco_vendor_packing_slip_line_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_vendor_packing_slip_line_lineage') }}
),
final as(
    select 
    opco_vendor_packing_slip_line_sk, opco_vendor_packing_slip_line_ck,opco_vendor_packing_slip_line_ak, crt_dtm, 
    STG_LOAD_DTM, DELETE_DTM,
    src_sys_nm, opco_id, packing_slip_id, internal_packing_slip_id, invtry_trans_id, opco_vendor_packing_slip_sk, opco_sk, opco_item_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_po_sk, orig_po_sk, opco_assctn_sk, opco_uom_sk, opco_invtry_ref_type_sk, line_nbr, dlvry_dt, invtry_trans_dt, invtry_ref_id, vendor_item_id, item_desc, partial_dlvry_ind, dest_country_nm, dest_state_nm, dest_county_nm, per_unit_qty, ordr_qty, received_qty, remaining_qty, invtry_qty, opco_currency_trans_amt


    from opco_vendor_packing_slip_line 
    where rnk = 1 and delete_dtm is null
)

select * from final