

      create or replace  table OI_DATA_DEV_V2.FND_DEV_BKP.opco_vendor_invoice  as
      (with opco_vendor_invoice as(
    select *,
    rank() over(partition by opco_vendor_invoice_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_vendor_invoice_lineage
),
final as(
    select 
    OPCO_VENDOR_INVOICE_sk, OPCO_VENDOR_INVOICE_ck,opco_vendor_invoice_ak, crt_dtm, 
    STG_LOAD_DTM, DELETE_DTM,
    src_sys_nm, opco_id, invoice_nbr, internal_invoice_nbr, opco_sk, opco_po_sk, opco_vendor_sk, opco_invoice_vendor_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_po_type_sk, trans_currency_sk, opco_pymnt_terms_sk, opco_cash_dscnt_terms_sk, opco_dlvry_terms_sk, opco_dlvry_mode_sk, opco_project_sk, vendor_grp_cd, invoice_dt, due_dt, gl_voucher_nbr, document_nbr, document_dt, cash_dscnt_dt, default_country_nm, purchaser_nm, invoice_qty, invoice_weight_amt, invoice_vol_amt, rqstd_by_nm, exchg_rt, pymnt_id, tax_grp_id, item_buyer_grp_id, fixed_due_dt, trans_currency_invoice_amt, opco_currency_invoice_amt, trans_currency_invoice_roundoff_amt, trans_currency_misc_chrgs_amt, opco_currency_misc_chrgs_amt, trans_currency_cash_dscnt_amt, trans_currency_line_dscnt_amt, trans_currency_balance_sls_amt


    from opco_vendor_invoice
    where rnk = 1 and delete_dtm is null
)

select * from final
      );
    