with opco_cust_invoice as(
    select *,
    rank() over(partition by opco_cust_invoice_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_cust_invoice_lineage') }}
),
final as(
    select 
    opco_cust_invoice_sk, opco_cust_invoice_ck, opco_cust_invoice_ak, CRT_DTM, STG_LOAD_DTM, DELETE_DTM,
    src_sys_nm, src_key_txt, opco_sk, opco_cust_sk, opco_sls_ordr_sk, opco_picking_list_sk, opco_cust_packing_slip_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk,
    opco_lob_sk, opco_sls_ordr_type_sk, invoice_cust_sk, trans_currency_sk, opco_cust_grp_sk, opco_pymnt_terms_sk, opco_cash_dscnt_terms_sk, opco_dlvry_terms_sk, opco_dlvry_mode_sk, dlvry_locn_sk
    invoice_locn_sk, invoice_nbr, trans_ref_txt, invoice_dt, due_dt, cash_dscnt_dt, gl_voucher_nbr, document_nbr, document_dt, dlvry_cust_nm, invoice_cust_nm, sls_grp_txt, sls_tax_included_ind,
    updt_after_printing_ind, specific_action_txt, dispatch_locn_txt, pymnt_sched_cd, lead_cd, po_form_nbr,commsn_grp_id, credit_reason_id, end_cust_id, tax_grp_id, invoice_qty, invoice_volume_amt,
    invoice_weight_amt, trans_currency_balance_sls_amt, opco_currency_balance_sls_amt, trans_currency_invoice_amt, opco_currency_invoice_amt, trans_currency_line_dscnt_amt, opco_currency_line_dscnt_amt,
    trans_currency_cash_dscnt_amt, trans_currency_sls_tax_amt, opco_currency_sls_tax_amt, trans_currency_misc_chrgs_amt, opco_currenty_misc_chrgs_amt, dlvry_cost_amt

    from opco_cust_invoice
    where rnk = 1 and delete_dtm is null
)

select * from final

