

      create or replace  table OI_DATA_DEV_V2.FND_DEV_BKP.opco_po  as
      (with opco_po as(
    select *,
    rank() over(partition by opco_po_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_po_lineage
),
final as(
    select 
     opco_po_sk, opco_po_ck,opco_po_ak, CRT_DTM, STG_LOAD_DTM, DELETE_DTM,
    src_sys_nm, opco_id, po_id, po_nm, opco_sk, opco_cost_center_sk, opco_dept_sk,opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_vendor_sk,
    invoice_vendor_sk, opco_cash_dscnt_terms_sk, trans_currency_sk, opco_trans_status_sk, opco_po_type_sk, opco_pymnt_terms_sk, opco_pymnt_mode_sk,
    intercompany_orig_sls_ordr_sk, intercompany_orig_cust_sk, opco_dlvry_mode_sk,  opco_dlvry_terms_sk, opco_po_doc_status_sk, warehouse_sk,
    opco_project_sk, opco_locn_sk, dlvry_dt, po_crt_dtm, po_reqstd_by_nm, vendor_grp_cd, dlvry_vendor_nm, vendor_ref_txt, alias_nm, item_buyer_grp_id,
    purchased_by_txt, taxable_ind, dscnt_pct, cash_dscnt_pct

    from opco_po
    where rnk = 1 and delete_dtm is null
)

select * from final
      );
    