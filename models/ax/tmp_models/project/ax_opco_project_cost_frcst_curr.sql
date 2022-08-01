with ax_opco_project_cost_frcst as(
    select *,
    rank() over(partition by opco_project_cost_frcst_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('ax_opco_project_cost_frcst') }}
),
final as(
    select 
    *
    from ax_opco_project_cost_frcst
    where rnk = 1 and delete_dtm is null
)

select * from final