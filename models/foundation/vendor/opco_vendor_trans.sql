with opco_vendor_trans  as(
    select *,
    rank() over(partition by opco_vendor_trans_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_vendor_trans_lineage') }}
),
final as(
    select 
    opco_vendor_trans_sk, opco_vendor_trans_ck, opco_vendor_trans_ak, crt_dtm, 
    STG_LOAD_DTM, DELETE_DTM,
    src_sys_nm, opco_id, src_key_txt, opco_sk, opco_vendor_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_po_sk, opco_gl_trans_type_sk, trans_currency_sk, opco_pymnt_mode_sk, opco_cash_dscnt_terms_sk, ltst_settled_opco_sk, ltst_settled_vendor_sk, trans_desc, trans_dt, due_dt, trans_close_dt, voucher_nbr, invoice_nbr, ltst_settled_voucher_nbr, ltst_settle_dt, trans_apprvd_ind, trans_apprvd_by_txt, trans_apprvd_dtm, document_nbr, document_dt, offset_src_key_txt, project_contract_orign_ind, crrctn_trans_ind, auto_settle_ind, cancelled_ind, pymnt_ref_txt, pymnt_id, trans_currency_trans_amt, trans_currency_settled_amt, opco_currency_trans_amt, opco_currency_settled_amt, exchg_rt, ltst_exchg_adjstmt_dt, opco_currency_exchg_adjstmt_amt, opco_currency_realized_exchg_adjstmt_amt, opco_currency_tax_amt, opco_currency_settled_tax_amt, dscnt_dt, dscnt_amt


    from opco_vendor_trans 
    where rnk = 1 and delete_dtm is null
)

select * from final