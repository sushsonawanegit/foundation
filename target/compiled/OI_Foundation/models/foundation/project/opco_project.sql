with opco_project as(
    select *,
    rank() over(partition by opco_project_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_project_lineage
),
final as(
    select 
    OPCO_project_SK, OPCO_project_CK, CRT_DTM, STG_LOAD_DTM, DELETE_DTM, src_sys_nm, opco_id, project_id, opco_sk, opco_cust_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_pymnt_terms_sk, dlvry_locn_sk, project_nm, project_status_txt, project_type_txt, project_crt_dt, project_start_dt, project_end_dt, estimated_start_dt, estimated_end_dt, actual_start_dt, actual_end_dt, estimator_nm, project_mgr_nm, gl_posting_ind, sort_1_id, sort_2_id, sort_3_id, sls_grp_txt, tax_grp_id, locn_1_txt, locn_2_txt, retention_terms_txt, retention_due_dt, use_tax_ind

    from opco_project
    where rnk = 1 and delete_dtm is null
)

select * from final