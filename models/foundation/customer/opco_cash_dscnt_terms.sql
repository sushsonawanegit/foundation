with opco_cash_dscnt_terms as(
    select *,
    rank() over(partition by opco_cash_dscnt_terms_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_cash_dscnt_terms_lineage') }}
),
final as(
    select 
    opco_CASH_DSCNT_TERMS_sk, opco_CASH_DSCNT_TERMS_ck, crt_dtm, stg_load_dtm, delete_dtm,
    src_sys_nm, src_cash_dscnt_terms_cd, src_cash_dscnt_terms_desc,pymnt_period_in_days_nbr,dscnt_pct
    from opco_cash_dscnt_terms
    where rnk = 1 and delete_dtm is null
)

select * from final