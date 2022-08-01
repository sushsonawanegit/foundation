with opco_vendor_invoice_line as(
    select *,
    rank() over(partition by opco_vendor_invoice_line_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_vendor_invoice_line_lineage
),
final as(
    select 
    OPCO_VENDOR_invoice_line_sk, OPCO_VENDOR_invoice_line_ck,opco_vendor_invoice_line_ak, crt_dtm, 
    STG_LOAD_DTM, DELETE_DTM,
    src_sys_nm, opco_id, invoice_nbr, internal_invoice_nbr, invtry_trans_id, opco_vendor_invoice_sk, opco_sk, opco_item_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_po_sk, orig_po_sk, gl_acct_sk, trans_currency_sk, opco_assctn_sk, opco_invtry_ref_type_sk, opco_uom_sk, line_nbr, invoice_dt, item_desc, vendor_item_id, invtry_ref_id, invtry_ref_trans_id, invtry_trans_dt, dest_country_nm, dest_state_nm, dest_county_nm, tax_grp_id, tax_item_grp_id, per_unit_qty, purchase_qty, received_qty, invtry_qty, dscnt_pct, line_dscnt_pct, per_unit_price_amt, trans_currency_trans_amt, opco_currency_trans_amt, trans_currency_misc_chrgs_amt, trans_currency_dscnt_amt, trans_currency_line_dscnt_amt, trans_currency_multi_line_dscnt_amt, trans_currency_tax_amt, opco_currency_tax_1099_amt


    from opco_vendor_invoice_line
    where rnk = 1 and delete_dtm is null
)

select * from final