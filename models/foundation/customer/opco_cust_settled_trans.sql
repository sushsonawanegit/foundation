with opco_cust_settled_trans as(
    select *,
    rank() over(partition by opco_cust_settled_trans_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_cust_settled_trans_lineage') }}
),
final as(
    select 
    opco_cust_settled_trans_sk, opco_cust_settled_trans_ck, opco_cust_settled_trans_ak, CRT_DTM, STG_LOAD_DTM, DELETE_DTM,
    src_sys_nm, opco_id, src_key_txt, opco_sk, opco_cust_trans_sk, opco_cust_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk,
    cash_dscnt_gl_acct_sk, trans_dt, due_dt, src_crt_dtm, offset_cust_sk, offset_cust_trans_sk, offset_voucher_nbr, cust_cash_dscnt_dt, latest_interest_calc_dt, 
    reversible_ind, settlement_grp_txt, settled_voucher_nbr, trans_currency_settled_amt, opco_currency_settled_amt, exch_adjstmnt_amt, utilized_cash_dscnt_amt

    from opco_cust_settled_trans
    where rnk = 1 and delete_dtm is null
)

select * from final

