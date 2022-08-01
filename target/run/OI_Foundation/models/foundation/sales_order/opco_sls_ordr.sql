

      create or replace  table OI_DATA_DEV_V2.FND_DEV_BKP.opco_sls_ordr  as
      (with opco_sls_ordr as(
    select *,
    rank() over(partition by opco_sls_ordr_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_sls_ordr_lineage
),
final as(
    select 
    opco_sls_ordr_sk, opco_sls_ordr_ck, opco_sls_ordr_ak, CRT_DTM, STG_LOAD_DTM, DELETE_DTM,
    src_sys_nm, opco_id, sls_ordr_id, sls_ordr_nm, sls_ordr_dt, ship_dt, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk,
    opco_lob_sk, opco_cust_sk, opco_trans_status_sk, opco_cust_grp_sk, opco_site_sk, opco_dlvry_terms_sk, opco_dlvry_mode_sk, dlvry_loc_sk,
    warehouse_sk, sls_grp_nm, tax_grp_id, quotation_id, cust_po_nbr, trans_currency_sk, opco_pymnt_terms_sk, opco_pymnt_mode_sk, opco_cash_dscnt_terms_sk,
    cash_dscnt_pct, sls_ordr_crt_dtm, sls_ordr_modified_dtm, sls_ordr_crt_by_nm, sls_ordr_modified_by_nm, on_hold_ind, on_hold_by_nm, on_hold_rsn_desc, 
    dscnt_pct, end_cust_nm, cust_ref_txt, dlvry_cust_nm, opco_sls_ordr_doc_status_sk, invoice_cust_sk, line_dscnt_cd, sls_ordr_phase_cd, trade_grp_cd,
    trade_grp_nm, trade_grp_module_txt, trade_grp_type_txt, rcpt_cnfrm_dt, rcpt_rqst_dt, sls_taker_nm, opco_sls_ordr_type_sk, ship_cnfrm_dt, 
    ship_rqst_dt, titan_job_nbr, online_ordr_ind, spcl_instr_txt, tax_exempt_nbr, commsn_grp_id, commsn_grp_nm, last_acceptance_dt, fixed_due_dt, 
    backlog_cost_amt, backlog_sls_amt, bid_dt, print_invoice_ind, sls_lead_cd, cust_alt_nm, dsptch_locn_nm, equipment_type_txt
    
    from opco_sls_ordr
    where rnk = 1 and delete_dtm is null
)

select * from final
      );
    