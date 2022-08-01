with ax_opco_project as(
    select *,
    rank() over(partition by opco_project_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('ax_opco_project') }}
),
final as(
    select 
    *
    from ax_opco_project
    where rnk = 1 and delete_dtm is null
)

select * from final