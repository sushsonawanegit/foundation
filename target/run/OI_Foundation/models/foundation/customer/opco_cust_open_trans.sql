

      create or replace  table OI_DATA_DEV_V2.FND_DEV_BKP.opco_cust_open_trans  as
      (with opco_cust_open_trans as(
    select *,
    rank() over(partition by opco_cust_open_trans_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_cust_open_trans_lineage
),
final as(
    select 
    opco_cust_open_trans_sk, opco_cust_open_trans_CK, opco_cust_open_trans_ak, CRT_DTM,  STG_LOAD_DTM, DELETE_DTM,
    src_sys_nm, opco_id, src_key_txt, opco_cust_trans_sk, opco_sk, opco_cust_sk, cash_dscnt_gl_acct_sk, trans_dt, due_dt, ltst_interest_calc_dt, cash_dscnt_dt, 
    cash_dscnt_usage_txt, trans_currency_trans_amt, opco_currency_trans_amt, included_cash_dscnt_amt 
    from opco_cust_open_trans
    where rnk = 1 and delete_dtm is null
)

select * from final
      );
    