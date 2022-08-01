

      create or replace  table OI_DATA_DEV_V2.FND_DEV_BKP.opco_project_empl_frcst  as
      (with opco_project_empl_frcst as(
    select *,
    rank() over(partition by opco_project_empl_frcst_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_project_empl_frcst_lineage
),
final as(
    select 
    opco_project_empl_frcst_sk, opco_project_empl_frcst_ck, opco_project_empl_frcst_ak, crt_dtm, STG_LOAD_DTM, DELETE_DTM,
    src_sys_nm, opco_id, project_empl_frcst_id, opco_project_sk, opco_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_project_catgry_sk, trans_currency_sk, opco_uom_sk, line_nbr, line_property_id, empl_id, model_id, tax_grp_id, include_amt_ind, lock_ind, sched_link_type_txt, sched_link_txt, sched_from_dt, sched_worktime_ind, sched_capacity_ind, revenue_pymnt_expctd_dt, cost_pymnt_expctd_dt, invoice_expctd_dt, eliminatin_expctd_dt, trans_hours_qty, sched_load_pct, trans_currency_cost_price_amt, trans_currency_sls_price_amt, total_revenue_amt, total_cost_amt, margin_contribution_pct


    from opco_project_empl_frcst
    where rnk = 1 and delete_dtm is null
)

select * from final
      );
    