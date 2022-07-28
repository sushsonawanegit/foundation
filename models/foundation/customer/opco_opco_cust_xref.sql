with opco_opco_cust_xref as(
    select *,
    rank() over(partition by opco_sk, opco_cust_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_opco_cust_xref_lineage') }}
),
final as(
    select 
    opco_sk, opco_cust_sk, CRT_DTM, STG_LOAD_DTM, DELETE_DTM,
    actv_ind, opco_currency_sk, opco_pymnt_terms_sk, opco_pymnt_mode_sk, opco_cust_code_sk, opco_cust_type_sk, opco_commsn_grp_sk,
    opco_cust_grp_sk, opco_cash_dscnt_terms_sk, line_disc_txt, max_cr_amt, mandatory_cr_lmt_ind, block_lvl_cd, cust_clssfctn_cd,
    pymnt_schd_desc, sls_grp_nm, rgnl_sls_mgr_nm, invoice_acct_nbr, segment_cd, segmentation_txt
    from opco_opco_cust_xref
    where rnk = 1 and delete_dtm is null
)

select * from final