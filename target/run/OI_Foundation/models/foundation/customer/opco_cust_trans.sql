

      create or replace  table OI_DATA_DEV_V2.FND_DEV_BKP.opco_cust_trans  as
      (with opco_cust_trans as(
    select *,
    rank() over(partition by opco_cust_trans_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_cust_trans_lineage
),
final as(
    select 
    opco_cust_trans_sk, opco_cust_trans_ck, opco_cust_trans_ak, CRT_DTM, STG_LOAD_DTM, DELETE_DTM,
    src_sys_nm, opco_id, src_key_txt, opco_sk, opco_cust_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_sls_ordr_sk, opco_gl_trans_type_sk,
    trans_currency_sk, opco_pymnt_mode_sk, opco_pymnt_terms_sk, opco_cash_dscnt_terms_sk, opco_dlvry_mode_sk, opco_project_sk, trans_dt, trans_apprvd_ind,
    voucher_nbr, invoice_nbr, offset_src_key_txt, due_dt, crrctn_trans_ind, auto_settle_ind, pymnt_cancelled_ind, interest_calc_allowed_ind, collection_letter_allowed_ind,
    cod_cash_pymnt_ind, pymnt_ref_txt, pymnt_method_txt, retention_cd, check_retention_ind, document_nbr, document_dt, trans_dscnt_dt, ltst_interest_dt,
    collection_letter_txt, po_form_nbr, ltst_settled_opco_sk, ltst_settled_cust_sk, ltst_settled_dt, ltst_settled_voucher_nbr, trans_closed_dt, trans_currency_trans_amt,
    opco_currency_trans_amt, trans_currency_total_settled_amt, opco_currency_total_settled_amt, trans_dscnt_amt, sum_tax_amt, trans_exch_rt
   
    from opco_cust_trans
    where rnk = 1 and delete_dtm is null
)

select * from final
      );
    