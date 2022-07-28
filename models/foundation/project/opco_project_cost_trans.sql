
with opco_project_cost_trans as(
    select *,
    rank() over(partition by opco_project_cost_trans_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_project_cost_trans_lineage') }}
),
final as(
    select 
    OPCO_PROJECT_COST_TRANS_sk, OPCO_PROJECT_COST_TRANS_ck, OPCO_PROJECT_COST_TRANS_ak, crt_dtm, STG_LOAD_DTM, DELETE_DTM,
    src_sys_nm, opco_id, project_cost_trans_id, opco_sk, opco_project_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_uom_sk, trans_currency_sk, opco_project_catgry_sk, opco_project_trans_status_sk, opco_project_trans_origin_sk, trans_dt, trans_desc, cost_gl_status_txt, tax_grp_id, voucher_nbr, invoice_nbr, document_nbr, ref_trans_id, calculated_ind, trans_qty, trans_currency_cost_price_amt, opco_currency_cost_price_amt, opco_currency_sls_price_amt, opco_currency_gl_sls_amt, opco_currency_gl_cost_amt, opco_currency_item_price_amt, opco_currency_total_std_cost_amt, opco_currency_total_cost_amt, opco_currency_total_revenue_amt, opco_currency_sls_tax_amt, opco_currency_freight_amt, opco_currency_cash_dscnt_amt, margin_contribution_pct


    from opco_project_cost_trans
    where rnk = 1 and delete_dtm is null
)

select * from final