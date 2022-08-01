

      create or replace  table OI_DATA_DEV_V2.FND_DEV_BKP.opco_cust_invoice_line  as
      (with opco_cust_invoice_line as(
    select *,
    rank() over(partition by opco_cust_invoice_line_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_cust_invoice_line_lineage
),
final as(
    select 
    opco_cust_invoice_line_sk, opco_cust_invoice_line_ck, opco_cust_invoice_line_ak, CRT_DTM, STG_LOAD_DTM, DELETE_DTM,
    src_sys_nm, opco_id, src_key_txt, opco_sk, opco_sls_ordr_sk, orig_sls_ordr_sk, opco_item_sk, opco_chart_of_accts_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk,
    opco_lob_sk, trans_currency_sk, sls_uom_sk, gl_prod_uom_sk,  gl_sls_uom_sk, opco_assctn_sk, opco_invtry_ref_type_sk, invoice_nbr, line_nbr, invtry_trans_id,  cust_item_id, tax_grp_id,
    tax_item_grp_id, invtry_ref_id, item_bom_id, item_desc, sls_grp_txt, shpmt_country_nm, dlvry_country_nm, dlvry_state_nm,  dlvry_county_nm, mark_txt, grp_nm, ship_dt, partial_dlvry_ind,
    risk_type_ind, kits_ind, commsn_calc_ind, dlvry_type_txt, invoice_line_txt, spcl_item_ind, spcl_in_spcl_ind, spcl_item_nbr, labor_hours_nbr, engineering_hours_nbr, item_weight_amt,
    per_unit_qty, invoice_item_qty, ordr_qty, dlvrd_qty, prev_invoiced_qty, remaining_qty, invtry_units_qty, gl_prod_qty, gl_sls_qty, sis_qty, dscnt_pct, line_dscnt_pct, 
    overhead_cost_pct, margin_contribution_pct, trans_currency_sls_amt, opco_currency_sls_amt, trans_currency_incl_sls_tax_amt, opco_currency_incl_sls_tax_amt, trans_currency_sum_line_dscnt_amt,
    opco_currency_sum_line_dscnt_amt, trans_currency_tax_amt, opco_currency_tax_amt, trans_currency_commsn_amt, opco_currency_commsn_amt, sls_markup_amt, per_unit_sls_price_amt, 
    online_price_amt, list_price_amt, imbedded_freight_cost_amt, imbedded_freight_amt, total_cost_amt, item_cost_amt, default_item_price_amt, item_price_amt

    from opco_cust_invoice_line
    where rnk = 1 and delete_dtm is null
)

select * from final
      );
    