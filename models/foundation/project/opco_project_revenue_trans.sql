with opco_project_revenue_trans as(
    select *,
    rank() over(partition by opco_project_revenue_trans_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_project_revenue_trans_lineage') }}
),
final as(
    select 
    opco_project_revenue_trans_sk, OPCO_PROJECT_REVENUE_TRANS_ck, OPCO_PROJECT_REVENUE_TRANS_AK, crt_dtm, stg_load_dtm, DELETE_DTM, 
    src_sys_nm, src_key_txt, opco_project_sk, opco_sk, chg_nbr, project_revenue_trans_type_cd, project_revenue_trans_desc, trans_dt, apprvd_ind, project_revenue_trans_amt


    from opco_project_revenue_trans
    where rnk = 1 and delete_dtm is null
)

select * from final