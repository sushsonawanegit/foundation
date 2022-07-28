with opco_project_empl_trans  as(
    select *,
    rank() over(partition by opco_project_empl_trans_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_project_empl_trans_lineage') }}
),
final as(
    select
    OPCO_PROJECT_EMPL_TRANS_sk, OPCO_PROJECT_EMPL_TRANS_ck, OPCO_PROJECT_EMPL_TRANS_ak, crt_dtm, stg_load_dtm, delete_dtm,
    src_sys_nm, opco_id, project_empl_trans_id, opco_project_sk, opco_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_project_catgry_sk, opco_project_trans_status_sk, opco_project_trans_origin_sk, trans_currency_sk, empl_id, line_property_id, ref_project_empl_trans_id, tax_grp_id, trans_dt, gl_voucher_nbr, gl_section_txt, project_empl_trans_desc, calc_ind, trans_hour_qty, opco_currency_hrly_cost_price_amt , trans_currency_hrly_sls_price_amt, item_price_amt, total_revenue_amt, total_cost_amt, total_std_cost_amt, margin_contribution_pct
    
    from opco_project_empl_trans 
    where rnk = 1 and delete_dtm is null
)

select * from final