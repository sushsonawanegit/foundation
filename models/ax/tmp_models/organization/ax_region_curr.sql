with ax_region as(
    select *,
    rank() over(partition by region_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('ax_region') }}
),
final as(
    select 
    *
    from ax_region
    where rnk = 1 and delete_dtm is null
)

select * from final