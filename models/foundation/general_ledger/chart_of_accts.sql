with chart_of_accts as(
    select *,
    rank() over(partition by chart_of_accts_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_chart_of_accts_lineage') }}
),
final as(
    select 
    chart_of_accts_sk, chart_of_accts_ck, crt_dtm, stg_load_dtm, delete_dtm, gl_acct_nbr, gl_acct_nm, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_chart_of_accts_type_sk, opco_uom_sk, gl_acct_alias_nm, blocked_in_journal_ind, dpo_exclusion_ind, movement_allowed_ind, cost_center_reqd_txt, dept_reqd_txt, type_reqd_txt, prnt_gl_acct_nbr, related_gl_acct_nbr, monetary_gl_acct_ind, actv_ind, balance_sheet_acct_ind, summary_acct_ind
    from chart_of_accts
    where rnk = 1 and delete_dtm is null
)

select * from final

