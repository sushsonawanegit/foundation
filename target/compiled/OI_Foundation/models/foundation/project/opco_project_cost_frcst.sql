with opco_project_cost_frcst as(
    select *,
    rank() over(partition by opco_project_cost_frcst_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_project_cost_frcst_lineage
),
final as(
    select 
    OPCO_project_cost_frcst_sK, OPCO_project_cost_frcst_CK, CRT_DTM, STG_LOAD_DTM, DELETE_DTM, src_sys_nm, opco_id, project_cost_frcst_id, opco_project_sk, opco_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_project_catgry_sk, trans_currency_sk, opco_uom_sk, line_property_id, invoice_expctd_dt, budget_amt_applctn_dt, revenue_pymnt_expctd_dt, cost_pymnt_expctd_dt, elimination_expctd_dt, frcst_model_id, tax_grp_id, include_in_rpt_ind, lock_ind, include_amt_ind, trans_qty, opco_currency_cost_price_amt, trans_currency_sls_price_amt, total_revenue_amt, total_cost_amt, marging_contribution_pct


    from opco_project_cost_frcst
    where rnk = 1 and delete_dtm is null
)

select * from final