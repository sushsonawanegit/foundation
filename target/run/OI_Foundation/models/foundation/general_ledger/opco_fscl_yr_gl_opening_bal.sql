

      create or replace  table OI_DATA_DEV_V2.FND_DEV_BKP.opco_fscl_yr_gl_opening_bal  as
      (with opco_fscl_yr_gl_opening_bal as(
    select *,
    rank() over(partition by opco_fscl_yr_gl_opening_bal_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_fscl_yr_gl_opening_bal_lineage
),
final as(
    select 
    opco_fscl_yr_gl_opening_bal_sk, opco_fscl_yr_gl_opening_bal_ck, opco_fscl_yr_gl_opening_bal_ak, crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_key_txt, opco_sk, opco_chart_of_accts_sk, opco_gl_trans_type_sk, opco_gl_trans_posting_type_sk, opco_cost_center_sk, opco_dept_sk,
    opco_type_sk, opco_purpose_sk, opco_lob_sk, trans_currency_sk, fscl_yr_strt_dt, fscl_yr_nbr, credit_ind, opening_bal_qty, trans_currency_opening_bal_amt,
    opco_currency_opening_bal_amt, opco_uom_sk, opco_brand_sk, opco_sub_ledger_type_sk, sub_ledger_cd
    from opco_fscl_yr_gl_opening_bal
    where rnk = 1 and delete_dtm is null
)

select * from final
      );
    