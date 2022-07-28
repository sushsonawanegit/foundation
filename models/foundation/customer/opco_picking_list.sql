with opco_picking_list as(
    select *,
    rank() over(partition by opco_picking_list_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_picking_list_lineage') }}
),
final as(
    select 
    opco_picking_list_sk, opco_picking_list_ck, opco_picking_list_ak, CRT_DTM, STG_LOAD_DTM, DELETE_DTM,
    src_sys_nm, opco_id, picking_list_id, shpmnt_id, opco_cust_sk, invoice_cust_sk, opco_invtry_trans_type_sk, opco_handling_status_sk, opco_dlvry_terms_sk,
    opco_dlvry_mode_sk, opco_site_sk, opco_pymnt_terms_sk, src_crt_dt, trans_ref_id, handling_type_txt, start_dtm, end_dtm, activation_dtm, rqstd_ship_dt,
    actl_ship_dt, cust_dlvry_dt, shpmnt_type_txt, dlvry_nm, credit_reason_id, po_form_nbr, tax_grp_id, picking_list_updt_ind, cust_ref_txt, print_invoice_ind,
    item_reserved_ind, packing_slip_available_ind, dlvry_window_txt, spcl_instr_txt, srvc_duration_mins_nbr, requrmnt_txt, picking_list_printed_ind, 
    picking_list_printed_after_chg_ind, do_not_send_ind
    from opco_picking_list
    where rnk = 1 and delete_dtm is null
)

select * from final

