

      create or replace  table OI_DATA_DEV_V2.FND_DEV_BKP.opco_project_trans_posting  as
      (with opco_project_trans_posting  as(
    select *,
    rank() over(partition by opco_project_trans_posting_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_project_trans_posting_lineage
),
final as(
    select 
    opco_project_trans_posting_sk, opco_project_trans_posting_ck,opco_project_trans_posting_AK ,crt_dtm, DELETE_DTM, 
    src_sys_nm, opco_id, src_key_txt, opco_project_sk, opco_sk, opco_item_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_project_catgry_sk, opco_project_trans_origin_sk, opco_gl_trans_origin_sk, opco_gl_trans_posting_type_sk, opco_gl_acct_sk, project_trans_posting_id, gl_trans_dt, voucher_nbr, project_trans_dt, project_trans_type_txt, project_type_txt, project_cost_or_sls_txt, project_adjstmt_ref_id, pymnt_dt, pymnt_status_txt, invtry_trans_id, empl_id, trans_qty, opco_currency_trans_amt


    from opco_project_trans_posting 
    where rnk = 1 and delete_dtm is null
)

select * from final
      );
    