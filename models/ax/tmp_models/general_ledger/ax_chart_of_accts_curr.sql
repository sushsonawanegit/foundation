with ax_chart_of_accts as(
    select *,
    rank() over(partition by chart_of_accts_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('ax_chart_of_accts') }}
),
final as(
    select 
    *
    from ax_chart_of_accts
    where rnk = 1 and delete_dtm is null
)

select * from final

