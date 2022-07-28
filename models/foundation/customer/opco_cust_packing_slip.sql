with opco_cust_packing_slip as(
    select *,
    rank() over(partition by opco_cust_packing_slip_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_cust_packing_slip_lineage') }}
),
final as(
    select 
    opco_cust_packing_slip_sk, opco_cust_packing_slip_ck, opco_cust_packing_slip_ak, CRT_DTM, STG_LOAD_DTM, DELETE_DTM,src_sys_nm,
    opco_id, packing_slip_id, opco_sls_ordr_sk, opco_sls_ordr_type_sk, opco_cust_sk, invoice_cust_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, 
    opco_lob_sk, opco_picking_list_sk, opco_site_sk, trans_currency_sk, opco_pymnt_terms_sk, warehouse_sk, opco_dlvry_terms_sk, opco_dlvry_mode_sk, dlvry_locn_sk, gl_voucher_nbr, 
    po_form_nbr, ship_dt, cust_ref_txt, tax_grp_id, print_invoice_ind, credit_reason_id, spcl_instr_txt, registered_Ind, dlvry_cust_nm, invoice_cust_nm, invoice_address_txt, 
    load_nbr, pl_load_cnt, miles_cnt, dlvry_qty, pallet_qty, pallet_rate_amt, dlvry_volume_amt, dlvry_weight_amt, dlvry_cost_amt, packing_slip_amt, invoice_nbr, invoice_dt,
    invoice_sent_ind, invoice_amt
    from opco_cust_packing_slip
    where rnk = 1 and delete_dtm is null
)

select * from final

